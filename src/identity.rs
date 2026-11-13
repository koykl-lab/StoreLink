//! Checked opaque identities and local generation-fence rules.
//!
//! These checks are not database admission: an adapter must condition the same
//! fence and resource state in the transaction that applies the effects.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentityError {
    #[error("{field} must contain 1..={max} UTF-8 bytes")]
    InvalidLength { field: &'static str, max: usize },
    #[error("{0} must be a non-nil UUID")]
    NilUuid(&'static str),
    #[error("generation must be positive")]
    ZeroGeneration,
    #[error("the literal null is not an immutable version ID")]
    NullVersion,
}

fn check_length(value: &str, field: &'static str, max: usize) -> Result<(), IdentityError> {
    if value.is_empty() || value.len() > max {
        return Err(IdentityError::InvalidLength { field, max });
    }
    Ok(())
}

pub(crate) fn check_uuid(value: Uuid, field: &'static str) -> Result<(), IdentityError> {
    if value.is_nil() {
        return Err(IdentityError::NilUuid(field));
    }
    Ok(())
}

macro_rules! opaque_label {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
                let value = value.into();
                check_length(&value, stringify!($name), 128)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

opaque_label!(DomainId);
opaque_label!(StoreId);
opaque_label!(NamespaceId);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "ScopeWire")]
pub struct Scope {
    domain: DomainId,
    incarnation: Uuid,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeWire {
    domain: DomainId,
    incarnation: Uuid,
    generation: u64,
}

impl TryFrom<ScopeWire> for Scope {
    type Error = IdentityError;

    fn try_from(value: ScopeWire) -> Result<Self, Self::Error> {
        Self::new(value.domain, value.incarnation, value.generation)
    }
}

impl Scope {
    pub fn new(
        domain: DomainId,
        incarnation: Uuid,
        generation: u64,
    ) -> Result<Self, IdentityError> {
        check_uuid(incarnation, "incarnation")?;
        if generation == 0 {
            return Err(IdentityError::ZeroGeneration);
        }
        Ok(Self {
            domain,
            incarnation,
            generation,
        })
    }

    pub fn domain(&self) -> &DomainId {
        &self.domain
    }
    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

macro_rules! scoped_id {
    ($name:ident, $wire:ident, $wire_path:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = $wire_path)]
        pub struct $name {
            scope: Scope,
            uuid: Uuid,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct $wire {
            scope: Scope,
            uuid: Uuid,
        }

        impl TryFrom<$wire> for $name {
            type Error = IdentityError;

            fn try_from(value: $wire) -> Result<Self, Self::Error> {
                Self::new(value.scope, value.uuid)
            }
        }

        impl $name {
            pub fn new(scope: Scope, uuid: Uuid) -> Result<Self, IdentityError> {
                check_uuid(uuid, stringify!($name))?;
                Ok(Self { scope, uuid })
            }

            pub fn scope(&self) -> &Scope {
                &self.scope
            }
            pub fn uuid(&self) -> Uuid {
                self.uuid
            }
        }
    };
}

scoped_id!(ResourceId, ResourceIdWire, "ResourceIdWire");
scoped_id!(CommandId, CommandIdWire, "CommandIdWire");

impl ResourceId {
    /// Derive the single temporary resource created by this v0 command. Both
    /// scope and UUID identify its creator; a separately issued resource nonce
    /// would permit recreation by a different command in the same generation.
    /// `new` and deserialization remain available for reading stored identities,
    /// but creation still requires `DomainFence::admit_creation` and transactional
    /// absence/non-replay guards.
    pub fn from_command(command: &CommandId) -> Self {
        Self {
            scope: command.scope().clone(),
            uuid: command.uuid(),
        }
    }
}

/// Exact immutable object identity. Equality always compares the full tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "VersionRefWire")]
pub struct VersionRef {
    store: StoreId,
    key: String,
    version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionRefWire {
    store: StoreId,
    key: String,
    version: String,
}

impl TryFrom<VersionRefWire> for VersionRef {
    type Error = IdentityError;

    fn try_from(value: VersionRefWire) -> Result<Self, Self::Error> {
        Self::new(value.store, value.key, value.version)
    }
}

impl VersionRef {
    pub fn new(
        store: StoreId,
        key: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, IdentityError> {
        let key = key.into();
        let version = version.into();
        check_length(&key, "key", 1024)?;
        check_length(&version, "version", 1024)?;
        if version == "null" {
            return Err(IdentityError::NullVersion);
        }
        Ok(Self {
            store,
            key,
            version,
        })
    }

    pub fn store(&self) -> &StoreId {
        &self.store
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn version(&self) -> &str {
        &self.version
    }

    /// SHA-256 over the fixed ASCII domain separator followed by store, key and
    /// version, each encoded as an unsigned 64-bit big-endian byte length then
    /// the exact UTF-8 bytes. This is only an index: compare the stored full
    /// `VersionRef` before acting, even when fingerprints match.
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"StoreLink/VersionRef/v0\0");
        for part in [
            self.store.as_str(),
            self.key.as_str(),
            self.version.as_str(),
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        hash.finalize().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FenceError {
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("fence requires compacted_through <= closed_through < current")]
    InvalidWatermarks,
    #[error("fence watermarks cannot move backwards")]
    Regression,
    #[error("generation overflow")]
    GenerationOverflow,
    #[error("domain or incarnation does not match the fence")]
    ScopeMismatch,
    #[error("command generation is closed or in the future")]
    GenerationNotAdmitted,
    #[error("resource creation generation must match the creating command")]
    CreationGenerationMismatch,
    #[error("resource UUID must match its creating command UUID")]
    CreatorMismatch,
    #[error("existing resource generation is in the future")]
    FutureResource,
}

/// Bounded local fence state, not an authoritative read or transaction guard.
/// Persist changes with an adapter-owned revision/CAS; do not overwrite a newer
/// fence using this snapshot. Compaction scheduling/retention is separate policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DomainFenceWire")]
pub struct DomainFence {
    domain: DomainId,
    incarnation: Uuid,
    current: u64,
    closed_through: u64,
    compacted_through: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DomainFenceWire {
    domain: DomainId,
    incarnation: Uuid,
    current: u64,
    closed_through: u64,
    compacted_through: u64,
}

impl TryFrom<DomainFenceWire> for DomainFence {
    type Error = FenceError;

    fn try_from(value: DomainFenceWire) -> Result<Self, Self::Error> {
        Self::new(
            value.domain,
            value.incarnation,
            value.current,
            value.closed_through,
            value.compacted_through,
        )
    }
}

impl DomainFence {
    pub fn new(
        domain: DomainId,
        incarnation: Uuid,
        current: u64,
        closed_through: u64,
        compacted_through: u64,
    ) -> Result<Self, FenceError> {
        check_uuid(incarnation, "incarnation")?;
        if compacted_through > closed_through || closed_through >= current {
            return Err(FenceError::InvalidWatermarks);
        }
        Ok(Self {
            domain,
            incarnation,
            current,
            closed_through,
            compacted_through,
        })
    }

    pub fn domain(&self) -> &DomainId {
        &self.domain
    }
    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }
    pub fn current(&self) -> u64 {
        self.current
    }
    pub fn closed_through(&self) -> u64 {
        self.closed_through
    }
    pub fn compacted_through(&self) -> u64 {
        self.compacted_through
    }

    pub fn advance_generation(&mut self) -> Result<u64, FenceError> {
        let next = self
            .current
            .checked_add(1)
            .ok_or(FenceError::GenerationOverflow)?;
        self.current = next;
        Ok(next)
    }

    pub fn close_through(&mut self, generation: u64) -> Result<(), FenceError> {
        if generation < self.closed_through {
            return Err(FenceError::Regression);
        }
        if generation >= self.current {
            return Err(FenceError::InvalidWatermarks);
        }
        self.closed_through = generation;
        Ok(())
    }

    /// Publish this watermark before deleting covered outcomes. The caller must
    /// separately prove the configured resolution-retention window has elapsed.
    pub fn compact_through(&mut self, generation: u64) -> Result<(), FenceError> {
        if generation < self.compacted_through {
            return Err(FenceError::Regression);
        }
        if generation > self.closed_through {
            return Err(FenceError::InvalidWatermarks);
        }
        self.compacted_through = generation;
        Ok(())
    }

    fn match_scope(&self, scope: &Scope) -> Result<(), FenceError> {
        if scope.domain != self.domain || scope.incarnation != self.incarnation {
            return Err(FenceError::ScopeMismatch);
        }
        Ok(())
    }

    pub fn admit_command(&self, command: &CommandId) -> Result<(), FenceError> {
        self.match_scope(command.scope())?;
        let generation = command.scope().generation();
        if generation <= self.closed_through || generation > self.current {
            return Err(FenceError::GenerationNotAdmitted);
        }
        Ok(())
    }

    /// Admission for creating a missing resource, never an upsert. V0 binds the
    /// one temporary resource created by a command to that command's exact scope
    /// and UUID. A different command cannot recreate the ID, even in the same
    /// still-open generation. The native transaction must also guard resource
    /// absence and command non-replay (including replay of the original creator).
    pub fn admit_creation(
        &self,
        command: &CommandId,
        resource: &ResourceId,
    ) -> Result<(), FenceError> {
        self.admit_command(command)?;
        self.match_scope(resource.scope())?;
        if resource.scope().generation() != command.scope().generation() {
            return Err(FenceError::CreationGenerationMismatch);
        }
        if resource.uuid() != command.uuid() {
            return Err(FenceError::CreatorMismatch);
        }
        Ok(())
    }

    /// Scope admission ONLY for an existing resource. The caller must read and
    /// condition its existence and valid state in the effects transaction. A
    /// missing/terminal resource must not be recreated using this method. Closed
    /// creation generations do not themselves invalidate existing live resources.
    pub fn admit_existing(
        &self,
        command: &CommandId,
        resource: &ResourceId,
    ) -> Result<(), FenceError> {
        self.admit_command(command)?;
        self.match_scope(resource.scope())?;
        if resource.scope().generation() > self.current {
            return Err(FenceError::FutureResource);
        }
        Ok(())
    }
}
