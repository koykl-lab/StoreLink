//! Receipt-free, application-owned terminal deletion. This is a trusted server
//! seam, not a schema interpreter, reference release, scheduler or history oracle.

use std::{collections::HashMap, fmt};

use aws_sdk_dynamodb::config::{Builder as NativeConfig, retry::RetryConfig};
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Get, TransactGetItem, TransactWriteItem};
use thiserror::Error;

use super::journal::{control_key, native_message};
use super::maintenance::{Control, FIELDS};
use super::pruning::{compacted, full_guard, write_error};
use super::{
    ApplicationActions, CommandJournal, JournalError, MaintenanceObservation, PruningError,
    PruningOutcome,
};
use crate::identity::{DomainFence, Scope};

type Item = HashMap<String, AttributeValue>;

/// Full coherent application/control evidence. Only this API can construct it;
/// no deserialization, caller-editable executable plan, or projected-read upgrade.
/// Consumed by pruning so that an ambiguous/conflicting attempt requires rereading.
pub struct ApplicationPruningObservation {
    target: Get,
    control: MaintenanceObservation,
    control_item: Item,
    item: Option<Item>,
}
impl ApplicationPruningObservation {
    /// Full application row for caller-owned schema/terminal-state validation.
    /// Absence is a current observation, never a historical outcome.
    pub fn item(&self) -> Option<&Item> {
        self.item.as_ref()
    }

    pub fn fence(&self) -> &DomainFence {
        self.control.fence()
    }
}
impl fmt::Debug for ApplicationPruningObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationPruningObservation")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum ApplicationPruningError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Pruning(#[from] PruningError),
    #[error("application pruning requires a full read without projection or expression aliases")]
    ProjectedRead,
    #[error("application pruning requires a nonempty native condition")]
    MissingCondition,
    #[error("application pruning requires a complete, nonempty retained-scope declaration")]
    MissingScopes,
    #[error("application pruning observation targets a different table/key or coordination domain")]
    TargetMismatch,
    /// Same conservative outcome classification as library terminal pruning, with
    /// native service diagnostics retained. Neither variant establishes rollback.
    #[error("{outcome}: {diagnostic}")]
    NativeWrite {
        #[source]
        outcome: PruningError,
        diagnostic: String,
    },
}

impl CommandJournal {
    /// Observe full control and an independently enrolled application item in one
    /// native transaction read. Missing/malformed control or incomplete responses
    /// fail closed. Native DynamoDB remains authoritative for key/schema syntax;
    /// errors (including validation errors) are never converted to absence.
    pub async fn observe_application_pruning_candidate(
        &self,
        get: Get,
    ) -> Result<ApplicationPruningObservation, ApplicationPruningError> {
        if get.projection_expression.is_some() || get.expression_attribute_names.is_some() {
            return Err(ApplicationPruningError::ProjectedRead);
        }
        let target = self
            .enrolled_tables()
            .qualify_application_read(get)
            .map_err(JournalError::from)?;
        let control = Get::builder()
            .table_name(self.coordination_arn())
            .set_key(Some(control_key(self.domain_id())))
            .build()
            .map_err(|e| JournalError::Corrupt(e.to_string()))?;
        let output = self
            .native_client()
            .transact_get_items()
            .set_transact_items(Some(vec![
                TransactGetItem::builder().get(control).build(),
                TransactGetItem::builder().get(target.clone()).build(),
            ]))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| {
                JournalError::Native(native_message(e.as_service_error(), e.to_string()))
            })?;
        let rows = output.responses();
        if rows.len() != 2 {
            return Err(PruningError::Corrupt.into());
        }
        let control = Control::parse(
            rows[0].item.clone(),
            self.domain_id(),
            self.coordination_arn(),
        )
        .map_err(PruningError::from)?;
        Ok(ApplicationPruningObservation {
            target,
            control: control.observation,
            control_item: rows[0].item.clone().ok_or(JournalError::MissingControl)?,
            item: rows[1].item.clone(),
        })
    }

    /// Delete one application-owned terminal record without minting a command,
    /// receipt, resource identity or time observation. The SDK's ephemeral native
    /// ClientRequestToken remains SDK-owned, not a durable StoreLink identity.
    /// Exactly one native
    /// write attempt combines the full observed-control guard and caller Delete.
    ///
    /// # Trusted application obligations
    ///
    /// Decode and validate the full supported schema, complete all counted input
    /// releases, and declare EVERY scope protecting this record (including both
    /// creation and terminal scopes). All must be behind the published compaction
    /// frontier or historically fenced by an earlier incarnation in this domain.
    ///
    /// The native condition MUST check terminal state and an ABA-safe immutable
    /// identity/mutation revision changed on EVERY semantic mutation, including
    /// added attributes and protected inputs. Equality of only observed attributes
    /// does not guard item shape. StoreLink cannot infer these schema obligations
    /// or evaluate arbitrary expressions; DynamoDB validates their native syntax,
    /// key schema, expression size and final transaction limits.
    ///
    /// Conflict/Unknown requires a new observation, never rollback/recreation.
    /// ObservationAbsent validates binding first and proves no historical result,
    /// legitimate prior pruning, cancellation or physical-object absence.
    pub async fn prune_application_item(
        &self,
        observation: ApplicationPruningObservation,
        delete: Delete,
        retained_scopes: &[Scope],
    ) -> Result<PruningOutcome, ApplicationPruningError> {
        let actions = self.application_pruning_actions(&observation, delete, retained_scopes)?;
        if observation.item.is_none() {
            return Ok(PruningOutcome::ObservationAbsent);
        }
        self.native_client()
            .transact_write_items()
            .set_transact_items(Some(actions))
            .customize()
            .config_override(NativeConfig::new().retry_config(RetryConfig::disabled()))
            .send()
            .await
            .map_err(|e| ApplicationPruningError::NativeWrite {
                outcome: write_error(e.as_service_error()),
                diagnostic: native_message(e.as_service_error(), e.to_string()),
            })?;
        Ok(PruningOutcome::Removed)
    }

    // Private assembly only; never exported as an executable-plan escape hatch.
    fn application_pruning_actions(
        &self,
        observation: &ApplicationPruningObservation,
        delete: Delete,
        retained_scopes: &[Scope],
    ) -> Result<Vec<TransactWriteItem>, ApplicationPruningError> {
        if observation.control.coordination_arn() != self.coordination_arn()
            || observation.fence().domain() != self.domain_id()
        {
            return Err(ApplicationPruningError::TargetMismatch);
        }
        if delete
            .condition_expression()
            .is_none_or(|c| c.trim().is_empty())
        {
            return Err(ApplicationPruningError::MissingCondition);
        }
        if retained_scopes.is_empty() {
            return Err(ApplicationPruningError::MissingScopes);
        }
        let application =
            ApplicationActions::new(vec![TransactWriteItem::builder().delete(delete).build()])
                .map_err(JournalError::from)?;
        let mut actions = self
            .enrolled_tables()
            .qualify_application(application)
            .map_err(JournalError::from)?
            .into_native();
        let delete = actions[0].delete.as_ref().ok_or(PruningError::Corrupt)?;
        if delete.table_name() != observation.target.table_name()
            || delete.key() != observation.target.key()
        {
            return Err(ApplicationPruningError::TargetMismatch);
        }
        for scope in retained_scopes {
            compacted(observation.fence(), scope)?;
        }
        let check = full_guard(self.coordination_arn(), &observation.control_item, &FIELDS)?;
        actions.insert(
            0,
            TransactWriteItem::builder().condition_check(check).build(),
        );
        // Apply the existing native-action structural/nesting bounds to the
        // complete two-action transaction, without inventing an expression VM.
        Ok(ApplicationActions::new(actions)
            .map_err(JournalError::from)?
            .into_native())
    }
}

#[cfg(test)]
mod tests;
