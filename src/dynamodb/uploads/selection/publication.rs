//! Journal-owned publication of selected uploads, with finite staging consumed
//! in the same native transaction as application actions and declared references.
//! This module performs no S3 I/O and does not supply DynamoDB with a server NOW.

use std::collections::HashSet;

use super::*;
use crate::dynamodb::references::{
    ReferenceChange, ReferenceError, ReferenceSnapshot, reference_actions_with_staged,
};

#[derive(Debug, Error)]
pub enum PublicationError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Upload(#[from] UploadError),
    #[error(transparent)]
    Reference(#[from] ReferenceError),
    #[error(transparent)]
    Claim(#[from] ClaimError),
    #[error("publication input list exceeds native transaction capacity")]
    TooManyInputs,
    #[error("duplicate selected intent, staging row or claim")]
    DuplicateSelection,
}

impl CommandJournal {
    /// Atomically publish declared reference transitions and consume selected
    /// staging claims, together with caller-authored native application actions.
    /// Snapshots are observations, not capabilities: full SELECTED intent and
    /// ACTIVE staging rows are rechecked by their terminal replacements. Each
    /// exact READY version receives ONE combined reference/claim-count action.
    ///
    /// Callers must declare EVERY affected managed pointer and provide all native
    /// authorization and concurrency conditions. There is no hidden-pointer
    /// coverage. Non-reused owner identities and application revisions must guard
    /// empty-slot ABA; absence of a library reference slot alone cannot do so.
    /// Reference revisions must never be reused across that slot's history.
    ///
    /// Unpublished staging remains finite; declared committed references are
    /// durable independently of the upload client's lifetime. Consuming unused
    /// staging is allowed for an application discard effect and makes an otherwise
    /// unprotected version eligible for GC. Independent use pins are not released.
    /// No namespace read is needed: immutable stage/intent binding is checked and
    /// command admission fences the domain/incarnation in the same transaction.
    ///
    /// A fresh trusted clock must establish claim validity before preparation.
    /// Passage of time is NOT a database transition: revocation/cancellation and
    /// publication contend on the same full rows. Retry only the frozen command;
    /// historical receipts and raw version tuples cannot replace current staging
    /// authority. The journal owns admission, result, digest and the FULL native
    /// transaction budget; no partial or caller-editable executable plan escapes.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_publish_staged(
        &self,
        context: CommandContext,
        application: ApplicationActions,
        changes: Vec<ReferenceChange>,
        sources: Vec<ReferenceSnapshot>,
        selected: Vec<(&StagedSnapshot, &IntentSnapshot)>,
        clock: ClockObservation,
        policy: &ClaimPolicy,
        result: Vec<u8>,
    ) -> Result<PreparedCommand, PublicationError> {
        bounded_inputs(&changes, &sources, &selected)?;
        for (stage, intent) in &selected {
            for id in [
                intent.id(),
                stage.intent_id(),
                stage.attempt_id(),
                stage.claim().id(),
            ] {
                self.existing_scope(&context, id)?;
            }
        }
        let internal = publication_actions(
            self.coordination_arn(),
            self.domain_id(),
            context.id().uuid(),
            &changes,
            &sources,
            &selected,
            clock,
            policy,
        )?;
        Ok(self.prepare_internal(context, application, internal, result)?)
    }
}

fn bounded_inputs(
    changes: &[ReferenceChange],
    sources: &[ReferenceSnapshot],
    selected: &[(&StagedSnapshot, &IntentSnapshot)],
) -> Result<(), PublicationError> {
    // Do not sum lengths: reference sources and changes can share guarded slots.
    // The journal checks the exact composed application/internal/receipt budget.
    if changes.len() > 97 || sources.len() > 97 || selected.len() > 97 {
        return Err(PublicationError::TooManyInputs);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn publication_actions(
    table: &str,
    domain: &DomainId,
    revision: Uuid,
    changes: &[ReferenceChange],
    sources: &[ReferenceSnapshot],
    selected: &[(&StagedSnapshot, &IntentSnapshot)],
    clock: ClockObservation,
    policy: &ClaimPolicy,
) -> Result<Vec<TransactWriteItem>, PublicationError> {
    bounded_inputs(changes, sources, selected)?;
    let mut intents = HashSet::new();
    let mut stages = HashSet::new();
    let mut claims = HashSet::new();
    for (stage, intent) in selected {
        if !intents.insert(intent.id())
            || !stages.insert(stage.intent_id())
            || !claims.insert(stage.claim().id())
        {
            return Err(PublicationError::DuplicateSelection);
        }
        staged_binding(stage)?;
        check_domain(domain, stage.intent_id())?;
        check_domain(domain, intent.id())?;
        if stage.intent_id() != intent.id()
            || stage.namespace_id() != intent.namespace_id()
            || stage.configuration_revision() != intent.configuration_revision()
            || stage.version().store() != intent.target().store()
            || stage.version().key() != intent.target().key()
            || revision.is_nil()
            || revision == intent.revision()
        {
            return Err(UploadError::Binding.into());
        }
        if intent.state() != IntentState::Selected {
            return Err(UploadError::Terminal.into());
        }
        stage.claim().validate_use(clock, policy)?;
    }
    let authoritative: Vec<_> = selected.iter().map(|(stage, _)| *stage).collect();
    let mut actions =
        reference_actions_with_staged(table, domain, changes, sources, &authoritative)?;
    for (stage, intent) in selected {
        let mut consumed = (*intent).clone();
        consumed.state = IntentState::Consumed;
        consumed.admission.revision = revision;
        let mut released = (*stage).clone();
        let transition = released.claim.release(stage.claim().revision())?;
        // validate_use above excludes terminal claims; a release therefore owns
        // exactly the one decrement already aggregated by the reference helper.
        debug_assert!(transition.accounting_must_change());
        actions.push(replace(
            table,
            encode_intent(intent)?,
            encode_intent(&consumed)?,
        )?);
        actions.push(replace(
            table,
            encode_staged(stage)?,
            encode_staged(&released)?,
        )?);
    }
    Ok(actions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamodb::{EnrolledTables, ReferenceId};
    use crate::identity::CommandId;

    fn ts(value: u64) -> TimestampMs {
        TimestampMs::new(value).unwrap()
    }
    fn clock(value: u64) -> ClockObservation {
        ClockObservation::trusted(ts(value), 5).unwrap()
    }
    fn policy() -> ClaimPolicy {
        ClaimPolicy::new(2000, 50, 10).unwrap()
    }
    fn resource(scope: &Scope, generation: u64) -> ResourceId {
        ResourceId::new(
            Scope::new(scope.domain().clone(), scope.incarnation(), generation).unwrap(),
            Uuid::new_v4(),
        )
        .unwrap()
    }
    struct Fixture {
        stage: StagedSnapshot,
        intent: IntentSnapshot,
        command: CommandId,
    }
    impl Fixture {
        fn new() -> Self {
            let scope = Scope::new(
                DomainId::new("publication-domain").unwrap(),
                Uuid::new_v4(),
                4,
            )
            .unwrap();
            let store = StoreId::new("publication-store").unwrap();
            let intent = IntentSnapshot {
                admission: Admission {
                    id: resource(&scope, 1),
                    namespace: NamespaceId::new("publication-namespace").unwrap(),
                    configuration_revision: Uuid::new_v4(),
                    target: UploadTarget::new(store.clone(), "managed/key").unwrap(),
                    constraints: UploadConstraints::new(100, None, None).unwrap(),
                    revision: Uuid::new_v4(),
                    // Intentionally expired: only the independent staging claim
                    // authorizes consumption, not renewal of upload admission.
                    deadline: ts(900),
                },
                state: IntentState::Selected,
            };
            let stage = StagedSnapshot {
                intent: intent.id().clone(),
                attempt: resource(&scope, 2),
                namespace: intent.namespace_id().clone(),
                configuration_revision: intent.configuration_revision(),
                claim: Claim::new(
                    resource(&scope, 3),
                    VersionRef::new(store, "managed/key", "selected-version").unwrap(),
                    Uuid::new_v4(),
                    ts(2500),
                    clock(1000),
                    &policy(),
                )
                .unwrap(),
            };
            Self {
                stage,
                intent,
                command: CommandId::new(scope, Uuid::new_v4()).unwrap(),
            }
        }
        fn attach(&self, slot: &str) -> ReferenceChange {
            ReferenceChange::new(
                ReferenceId::new(slot).unwrap(),
                None,
                Some(self.stage.version().clone()),
                Some(Uuid::new_v4()),
            )
            .unwrap()
        }
        fn build(
            &self,
            changes: &[ReferenceChange],
            sources: &[ReferenceSnapshot],
        ) -> Result<Vec<TransactWriteItem>, PublicationError> {
            publication_actions(
                "coord",
                self.command.scope().domain(),
                self.command.uuid(),
                changes,
                sources,
                &[(&self.stage, &self.intent)],
                clock(1000),
                &policy(),
            )
        }
    }

    fn assert_full_old(put: &aws_sdk_dynamodb::types::Put, old: &Item) {
        let expected = condition(old);
        assert_eq!(
            put.condition_expression(),
            Some(expected.expression.as_str())
        );
        assert_eq!(put.expression_attribute_names(), Some(&expected.names));
        assert_eq!(put.expression_attribute_values(), Some(&expected.values));
        assert_eq!(item_key(put.item()), item_key(old));
    }
    fn version_updates(actions: &[TransactWriteItem]) -> Vec<&aws_sdk_dynamodb::types::Update> {
        actions.iter().filter_map(|a| a.update.as_ref()).collect()
    }

    #[test]
    fn attach_and_release_preserve_reference_delta_in_one_version_action() {
        let f = Fixture::new();
        for count in [1, 2] {
            let changes: Vec<_> = (0..count).map(|i| f.attach(&format!("slot-{i}"))).collect();
            let actions = f.build(&changes, &[]).unwrap();
            assert_eq!(actions.len(), count + 3);
            let updates = version_updates(&actions);
            assert_eq!(updates.len(), 1);
            let update = updates[0];
            assert_eq!(
                update.update_expression(),
                "ADD #references :delta, #claims :claim_delta"
            );
            let values = update.expression_attribute_values().unwrap();
            assert_eq!(values[":delta"], n(count as u64));
            assert_eq!(values[":claim_delta"], AttributeValue::N("-1".into()));
            assert_eq!(values[":minimum_claims"], n(1));
            assert_eq!(values[":ready"], s("READY"));
            assert_eq!(values[":store"], s(f.stage.version().store().as_str()));
            assert_eq!(values[":key"], s(f.stage.version().key()));
            assert_eq!(values[":version"], s(f.stage.version().version()));
            let guard = update.condition_expression().unwrap();
            for term in [
                "#record = :record",
                "#state = :ready",
                "#claims >= :minimum_claims",
                "attribute_type(#references, :number)",
                "#references >= :zero",
                "attribute_type(#claims, :number)",
                "#claims >= :zero",
            ] {
                assert!(guard.contains(term), "missing {term}");
            }
            let intent_put = actions[count + 1].put.as_ref().unwrap();
            assert_full_old(intent_put, &encode_intent(&f.intent).unwrap());
            let consumed = decode_intent(f.intent.id(), intent_put.item()).unwrap();
            assert_eq!(consumed.state(), IntentState::Consumed);
            assert_eq!(consumed.revision(), f.command.uuid());
            let stage_put = actions[count + 2].put.as_ref().unwrap();
            assert_full_old(stage_put, &encode_staged(&f.stage).unwrap());
            let released = decode_staged(f.intent.id(), stage_put.item()).unwrap();
            let mut expected = f.stage.clone();
            expected.claim.release(expected.claim.revision()).unwrap();
            assert_eq!(released, expected); // ALL immutable fields/deadline retained
            assert_eq!(released.claim().state(), ClaimState::Released);
            // Expected ACTIVE/SELECTED full rows make cancellation, revocation,
            // renewal and stale re-publication conflict, without a server NOW.
            assert_ne!(
                encode_staged(&released).unwrap(),
                encode_staged(&f.stage).unwrap()
            );
        }
    }

    #[test]
    fn unused_stage_can_be_consumed_without_creating_references_or_touching_use_pins() {
        let f = Fixture::new();
        let actions = f.build(&[], &[]).unwrap();
        assert_eq!(actions.len(), 3);
        let update = &actions[0].update.as_ref().unwrap();
        assert_eq!(update.update_expression(), "ADD #claims :claim_delta");
        assert_eq!(
            update.expression_attribute_values().unwrap()[":claim_delta"],
            AttributeValue::N("-1".into())
        );
        assert!(
            !update
                .expression_attribute_values()
                .unwrap()
                .contains_key(":delta")
        );
        assert_eq!(actions.iter().filter(|a| a.put.is_some()).count(), 2);
    }

    #[test]
    fn duplicate_selections_are_rejected_even_when_identical() {
        let f = Fixture::new();
        for duplicate in ["all", "intent", "stage", "claim"] {
            let mut other = Fixture::new();
            other.stage = f.stage.clone();
            other.intent = f.intent.clone();
            match duplicate {
                "all" => {}
                "intent" => other.stage.intent = resource(f.command.scope(), 1),
                "stage" => other.intent.admission.id = resource(f.command.scope(), 1),
                "claim" => {
                    other.intent.admission.id = resource(f.command.scope(), 1);
                    other.stage.intent = other.intent.id().clone();
                    other.stage.attempt = resource(f.command.scope(), 2);
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    publication_actions(
                        "coord",
                        f.command.scope().domain(),
                        f.command.uuid(),
                        &[],
                        &[],
                        &[(&f.stage, &f.intent), (&other.stage, &other.intent)],
                        clock(1000),
                        &policy(),
                    ),
                    Err(PublicationError::DuplicateSelection)
                ),
                "accepted {duplicate}"
            );
        }
    }

    #[test]
    fn terminal_and_unusable_claims_fail_before_action_preparation() {
        for state in [
            IntentState::Open,
            IntentState::Cancelled,
            IntentState::Consumed,
        ] {
            let mut f = Fixture::new();
            f.intent.state = state;
            assert!(matches!(
                f.build(&[], &[]),
                Err(PublicationError::Upload(UploadError::Terminal))
            ));
        }
        for state in [ClaimState::Released, ClaimState::Revoked] {
            let mut f = Fixture::new();
            if state == ClaimState::Released {
                f.stage.claim.release(f.stage.claim.revision()).unwrap();
            } else {
                f.stage
                    .claim
                    .revoke(f.stage.claim.revision(), clock(2510), &policy())
                    .unwrap();
            }
            assert!(matches!(
                f.build(&[], &[]),
                Err(PublicationError::Claim(ClaimError::Terminal))
            ));
        }
        let f = Fixture::new();
        for observation in [
            clock(2495),
            clock(2600),
            ClockObservation::untrusted(ts(1000), 0),
            ClockObservation::trusted(ts(1000), 11).unwrap(),
        ] {
            assert!(
                publication_actions(
                    "coord",
                    f.command.scope().domain(),
                    f.command.uuid(),
                    &[],
                    &[],
                    &[(&f.stage, &f.intent)],
                    observation,
                    &policy(),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn stage_intent_configuration_namespace_target_and_generation_bindings_fail_closed() {
        for field in [
            "intent",
            "namespace",
            "config",
            "store",
            "key",
            "domain",
            "incarnation",
            "attempt_generation",
            "claim_generation",
            "revision",
        ] {
            let mut f = Fixture::new();
            match field {
                "intent" => f.intent.admission.id = resource(f.command.scope(), 1),
                "namespace" => f.intent.admission.namespace = NamespaceId::new("other").unwrap(),
                "config" => f.intent.admission.configuration_revision = Uuid::new_v4(),
                "store" => f.intent.admission.target.store = StoreId::new("other").unwrap(),
                "key" => f.intent.admission.target.key = "other/key".into(),
                "domain" | "incarnation" => {
                    let scope = Scope::new(
                        if field == "domain" {
                            DomainId::new("other").unwrap()
                        } else {
                            f.command.scope().domain().clone()
                        },
                        if field == "incarnation" {
                            Uuid::new_v4()
                        } else {
                            f.command.scope().incarnation()
                        },
                        2,
                    )
                    .unwrap();
                    f.stage.attempt = resource(&scope, 2);
                }
                "attempt_generation" => f.stage.attempt = resource(f.command.scope(), 4),
                "claim_generation" => {
                    f.stage.claim = Claim::new(
                        resource(f.command.scope(), 1),
                        f.stage.version().clone(),
                        Uuid::new_v4(),
                        ts(2500),
                        clock(1000),
                        &policy(),
                    )
                    .unwrap();
                }
                "revision" => f.intent.admission.revision = f.command.uuid(),
                _ => unreachable!(),
            }
            assert!(f.build(&[], &[]).is_err(), "accepted {field}");
        }
        let f = Fixture::new();
        let unrelated = ReferenceChange::new(
            ReferenceId::new("slot").unwrap(),
            None,
            Some(
                VersionRef::new(
                    f.stage.version().store().clone(),
                    "managed/key",
                    "raw-untracked",
                )
                .unwrap(),
            ),
            Some(Uuid::new_v4()),
        )
        .unwrap();
        assert!(matches!(
            f.build(&[unrelated], &[]),
            Err(PublicationError::Reference(ReferenceError::MissingSource))
        ));
    }

    // Loopback wire fixtures only: the native SDK decodes observations through
    // production readers. This neither evaluates expressions nor proves AWS
    // atomicity. No public fake receipt/snapshot/context constructor is added.
    async fn wire_fixture(
        f: &Fixture,
        contexts: usize,
        source_version: &VersionRef,
    ) -> (CommandJournal, Vec<CommandContext>, ReferenceSnapshot) {
        use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::Duration;

        let description = |name: &str| {
            serde_json::json!({"Table": {
                "TableName": name,
                "TableArn": format!("arn:aws:dynamodb:us-west-2:123456789012:table/{name}"),
                "TableStatus": "ACTIVE",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}]
            }})
        };
        let domain = f.command.scope().domain();
        let control = serde_json::json!({"Item": {
            "pk": {"S": format!("{}#CONTROL", prefix(domain))}, "sk": {"S": "M"},
            "domain": {"S": domain.as_str()},
            "incarnation": {"S": f.command.scope().incarnation().to_string()},
            "current_generation": {"N": f.command.scope().generation().to_string()},
            "closed_through": {"N": "0"}, "compacted_through": {"N": "0"}, "revision": {"N": "1"}
        }});
        let source = serde_json::json!({"Item": {
            "pk": {"S": format!("{}#REF#736f75726365", prefix(domain))}, "sk": {"S": "M"},
            "record": {"S": "reference"}, "reference_id": {"S": "source"},
            "revision": {"S": Uuid::new_v4().to_string()},
            "store": {"S": source_version.store().as_str()}, "object_key": {"S": source_version.key()},
            "version_id": {"S": source_version.version()}
        }});
        let mut responses = vec![description("coord"), description("app"), source];
        responses.extend(std::iter::repeat_n(control, contexts));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&buffer[..count]);
                    assert!(request.len() < 65_536);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let body = response.to_string();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let client = aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-west-2"))
                .credentials_provider(Credentials::new(
                    "synthetic",
                    "synthetic",
                    None,
                    None,
                    "publication-unit",
                ))
                .endpoint_url(endpoint)
                .build(),
        );
        let tables = EnrolledTables::enroll(&client, "coord", &["app".into()])
            .await
            .unwrap();
        let journal = CommandJournal::new(client, tables, domain.clone());
        let source = journal
            .read_reference(&ReferenceId::new("source").unwrap())
            .await
            .unwrap()
            .unwrap();
        let mut observed = Vec::new();
        for _ in 0..contexts {
            observed.push(journal.command_context().await.unwrap());
        }
        server.join().unwrap();
        (journal, observed, source)
    }

    fn application(count: usize) -> ApplicationActions {
        ApplicationActions::new(
            (0..count)
                .map(|i| {
                    let put = Put::builder()
                        .table_name("app")
                        .item("pk", s(format!("application-{i}")))
                        .item("sk", s("M"))
                        .condition_expression("attribute_not_exists(pk)")
                        .build()
                        .unwrap();
                    TransactWriteItem::builder().put(put).build()
                })
                .collect(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn existing_reference_and_stage_authority_merge_and_replacement_guards_old_version() {
        let f = Fixture::new();
        let old_version = VersionRef::new(
            f.stage.version().store().clone(),
            "managed/key",
            "old-version",
        )
        .unwrap();
        let (_, _, old) = wire_fixture(&f, 0, &old_version).await;
        let replacement = ReferenceChange::new(
            old.id().clone(),
            Some(old.clone()),
            Some(f.stage.version().clone()),
            Some(Uuid::new_v4()),
        )
        .unwrap();
        let extra_old = ReferenceChange::new(
            ReferenceId::new("old-copy").unwrap(),
            None,
            Some(old.version().clone()),
            Some(Uuid::new_v4()),
        )
        .unwrap();
        let actions = f
            .build(
                &[replacement.clone(), extra_old],
                std::slice::from_ref(&old),
            )
            .unwrap();
        // Old -1/+1 is a READY condition; selected +1/-1 remains ONE update.
        assert_eq!(actions.len(), 6);
        assert_eq!(version_updates(&actions).len(), 1);
        let selected = version_updates(&actions)[0];
        assert_eq!(
            selected.update_expression(),
            "ADD #references :delta, #claims :claim_delta"
        );
        assert_eq!(
            selected.expression_attribute_values().unwrap()[":delta"],
            n(1)
        );
        assert_eq!(
            selected.expression_attribute_values().unwrap()[":claim_delta"],
            AttributeValue::N("-1".into())
        );
        assert!(actions.iter().any(|a| {
            a.condition_check.as_ref().is_some_and(|c| {
                c.expression_attribute_values().unwrap().get(":version") == Some(&s("old-version"))
            })
        }));
        let replacing = f.build(&[replacement], &[old]).unwrap();
        let updates = version_updates(&replacing);
        assert_eq!(updates.len(), 2);
        let old = updates
            .iter()
            .find(|a| a.expression_attribute_values().unwrap()[":version"] == s("old-version"))
            .unwrap();
        assert_eq!(old.update_expression(), "ADD #references :delta");
        assert_eq!(
            old.expression_attribute_values().unwrap()[":delta"],
            AttributeValue::N("-1".into())
        );
        assert!(
            old.condition_expression()
                .unwrap()
                .contains("#references >= :minimum")
        );

        let (_, _, same) = wire_fixture(&f, 0, f.stage.version()).await;
        let actions = f.build(&[f.attach("new-slot")], &[same]).unwrap();
        assert_eq!(version_updates(&actions).len(), 1); // source check is not a second version action
        assert_eq!(
            actions
                .iter()
                .filter(|a| a.condition_check.is_some())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn full_journal_budget_includes_application_admission_and_receipt() {
        let f = Fixture::new();
        let (journal, contexts, source) = wire_fixture(&f, 3, f.stage.version()).await;
        let mut contexts = contexts.into_iter();
        let prepare = |context, count| {
            journal.prepare_publish_staged(
                context,
                application(count),
                vec![f.attach("slot")],
                vec![],
                vec![(&f.stage, &f.intent)],
                clock(1000),
                &policy(),
                b"business-result".to_vec(),
            )
        };
        let command = prepare(contexts.next().unwrap(), 94).unwrap();
        assert_eq!(command.action_count(), 100); // 94 app + 4 internal + 2 journal
        assert_eq!(command.clone().token(), command.token());
        assert!(matches!(
            prepare(contexts.next().unwrap(), 95),
            Err(PublicationError::Journal(JournalError::Budget {
                count: 101
            }))
        ));
        assert!(matches!(
            journal.prepare_publish_staged(
                contexts.next().unwrap(),
                application(0),
                vec![],
                vec![source; 98],
                vec![],
                clock(1000),
                &policy(),
                vec![],
            ),
            Err(PublicationError::TooManyInputs)
        ));
    }

    #[tokio::test]
    async fn public_preparation_rejects_future_claim_and_cross_scope_resources() {
        for mismatch in [
            "claim_future",
            "attempt_future",
            "intent_future",
            "domain",
            "incarnation",
        ] {
            let mut f = Fixture::new();
            f.command = CommandId::new(
                Scope::new(
                    if mismatch == "domain" {
                        DomainId::new("other").unwrap()
                    } else {
                        f.command.scope().domain().clone()
                    },
                    if mismatch == "incarnation" {
                        Uuid::new_v4()
                    } else {
                        f.command.scope().incarnation()
                    },
                    match mismatch {
                        "claim_future" => 2,
                        "attempt_future" => 1,
                        _ => 4,
                    },
                )
                .unwrap(),
                Uuid::new_v4(),
            )
            .unwrap();
            if mismatch == "intent_future" {
                f.intent.admission.id = resource(f.command.scope(), 5);
            }
            let (journal, mut contexts, _) = wire_fixture(&f, 1, f.stage.version()).await;
            assert!(
                matches!(
                    journal.prepare_publish_staged(
                        contexts.remove(0),
                        application(0),
                        vec![],
                        vec![],
                        vec![(&f.stage, &f.intent)],
                        clock(1000),
                        &policy(),
                        vec![],
                    ),
                    Err(PublicationError::Upload(UploadError::Binding))
                ),
                "accepted {mismatch}"
            );
        }
    }

    #[test]
    fn input_bounds_precede_authority_checks() {
        let f = Fixture::new();
        let changes = vec![f.attach("duplicate-slot"); 98];
        assert!(matches!(
            f.build(&changes, &[]),
            Err(PublicationError::TooManyInputs)
        ));
        assert!(matches!(
            publication_actions(
                "coord",
                f.command.scope().domain(),
                f.command.uuid(),
                &[],
                &[],
                &vec![(&f.stage, &f.intent); 98],
                clock(1000),
                &policy(),
            ),
            Err(PublicationError::TooManyInputs)
        ));
    }
}
