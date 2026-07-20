//! `sync_together` manifest types.
//!
//! A `sync_together` group names a set of events that — during live indexing
//! only — advance block-by-block in lockstep: for each block, all grouped
//! events' Postgres writes (raw event rows, custom-table operations, reorg
//! journal, checkpoints) commit in one transaction per block, per network.
//!
//! Two YAML entry points produce groups:
//!
//! 1. Explicit top-level groups (cross-contract):
//! ```yaml
//! sync_together:
//!   - group: market-sync
//!     contracts:
//!       - name: CTFExchange
//!         events:
//!           - OrderFilled
//!       - name: ConditionalTokens
//!         events:
//!           - PositionSplit
//! ```
//!
//! 2. Per-table opt-in (`sync_together: true` on a custom table), which
//!    desugars into an implicit group of that table's events.

use serde::{Deserialize, Serialize};

use crate::event::contract_setup::ContractEventMapping;
use crate::manifest::core::Manifest;

/// Prefix for group names generated from table-level `sync_together: true`
/// flags. Implicit group names are never user-supplied.
pub const IMPLICIT_GROUP_PREFIX: &str = "table:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncTogetherGroup {
    /// Unique group name (used in logs and error messages).
    pub group: String,
    /// Member contracts with their events, in YAML order. Contract order ×
    /// event order defines the deterministic per-block callback order.
    pub contracts: Vec<SyncTogetherContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncTogetherContract {
    /// Contract name as declared under `contracts:` in the manifest.
    pub name: String,
    /// Event names on that contract.
    pub events: Vec<String>,
}

impl SyncTogetherGroup {
    /// Members flattened in YAML order.
    pub fn members(&self) -> Vec<ContractEventMapping> {
        self.contracts
            .iter()
            .flat_map(|contract| {
                contract.events.iter().map(|event| ContractEventMapping {
                    contract_name: contract.name.clone(),
                    event_name: event.clone(),
                })
            })
            .collect()
    }

    /// True if this group was generated from a table-level flag rather than
    /// declared under the top-level `sync_together:` key.
    pub fn is_implicit(&self) -> bool {
        self.group.starts_with(IMPLICIT_GROUP_PREFIX)
    }
}

/// Expands the manifest's explicit groups plus the implicit groups generated
/// by table-level `sync_together: true` flags into one list.
///
/// Implicit groups on the same contract that share an event are auto-merged
/// (they would race on the same callback otherwise). A conflict between an
/// explicit group and an implicit group is NOT resolved here — validation
/// rejects it with a dedicated error so the user folds the table's events into
/// the explicit group.
pub fn effective_sync_together_groups(manifest: &Manifest) -> Vec<SyncTogetherGroup> {
    let mut groups: Vec<SyncTogetherGroup> = manifest.sync_together.clone().unwrap_or_default();

    for contract in &manifest.contracts {
        let Some(tables) = &contract.tables else { continue };

        let mut contract_groups: Vec<SyncTogetherGroup> = Vec::new();
        for table in tables.iter().filter(|t| t.sync_together) {
            contract_groups.push(SyncTogetherGroup {
                group: format!("{}{}.{}", IMPLICIT_GROUP_PREFIX, contract.name, table.name),
                contracts: vec![SyncTogetherContract {
                    name: contract.name.clone(),
                    events: table.events.iter().map(|e| e.event.clone()).collect(),
                }],
            });
        }

        merge_overlapping_implicit_groups(&mut contract_groups);
        groups.extend(contract_groups);
    }

    groups
}

/// Merges implicit groups (all on one contract) that share any event, to a
/// fixed point so transitive overlaps collapse into one group.
fn merge_overlapping_implicit_groups(groups: &mut Vec<SyncTogetherGroup>) {
    loop {
        let mut merged_any = false;

        'outer: for i in 0..groups.len() {
            for j in (i + 1)..groups.len() {
                let shares_event = groups[i].contracts[0]
                    .events
                    .iter()
                    .any(|e| groups[j].contracts[0].events.contains(e));

                if shares_event {
                    let absorbed = groups.remove(j);
                    let target = &mut groups[i].contracts[0];
                    for event in absorbed.contracts[0].events.iter() {
                        if !target.events.contains(event) {
                            target.events.push(event.clone());
                        }
                    }
                    merged_any = true;
                    break 'outer;
                }
            }
        }

        if !merged_any {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_contract_grouped_shape() {
        let yaml = r#"
- group: market-sync
  contracts:
    - name: CTFExchange
      events:
        - OrderFilled
    - name: ConditionalTokens
      events:
        - PositionSplit
        - PositionsMerge
"#;
        let groups: Vec<SyncTogetherGroup> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group, "market-sync");
        assert!(!groups[0].is_implicit());

        let members = groups[0].members();
        assert_eq!(
            members,
            vec![
                ContractEventMapping {
                    contract_name: "CTFExchange".into(),
                    event_name: "OrderFilled".into()
                },
                ContractEventMapping {
                    contract_name: "ConditionalTokens".into(),
                    event_name: "PositionSplit".into()
                },
                ContractEventMapping {
                    contract_name: "ConditionalTokens".into(),
                    event_name: "PositionsMerge".into()
                },
            ]
        );
    }

    #[test]
    fn round_trips_through_serde() {
        let group = SyncTogetherGroup {
            group: "g".into(),
            contracts: vec![SyncTogetherContract {
                name: "C".into(),
                events: vec!["E1".into(), "E2".into()],
            }],
        };
        let yaml = serde_yaml::to_string(&group).unwrap();
        let back: SyncTogetherGroup = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(group, back);
    }

    #[test]
    fn rejects_missing_fields() {
        // events missing
        assert!(serde_yaml::from_str::<SyncTogetherGroup>("group: g\ncontracts:\n  - name: C\n")
            .is_err());
        // contracts missing
        assert!(serde_yaml::from_str::<SyncTogetherGroup>("group: g\n").is_err());
        // dotted-string members are NOT supported (draft syntax was dropped)
        assert!(serde_yaml::from_str::<SyncTogetherGroup>(
            "group: g\nevents:\n  - CTFExchange.OrderFilled\n"
        )
        .is_err());
    }

    #[test]
    fn merges_transitively_overlapping_implicit_groups() {
        let mk = |name: &str, events: &[&str]| SyncTogetherGroup {
            group: format!("{}C.{}", IMPLICIT_GROUP_PREFIX, name),
            contracts: vec![SyncTogetherContract {
                name: "C".into(),
                events: events.iter().map(|e| e.to_string()).collect(),
            }],
        };

        // A{e1}, B{e2} don't overlap; C{e1,e2} bridges them.
        let mut groups = vec![mk("a", &["e1"]), mk("b", &["e2"]), mk("c", &["e1", "e2"])];
        merge_overlapping_implicit_groups(&mut groups);

        assert_eq!(groups.len(), 1);
        let mut events = groups[0].contracts[0].events.clone();
        events.sort();
        assert_eq!(events, vec!["e1", "e2"]);
    }

    #[test]
    fn keeps_disjoint_implicit_groups_separate() {
        let mk = |name: &str, events: &[&str]| SyncTogetherGroup {
            group: format!("{}C.{}", IMPLICIT_GROUP_PREFIX, name),
            contracts: vec![SyncTogetherContract {
                name: "C".into(),
                events: events.iter().map(|e| e.to_string()).collect(),
            }],
        };

        let mut groups = vec![mk("a", &["e1"]), mk("b", &["e2"])];
        merge_overlapping_implicit_groups(&mut groups);
        assert_eq!(groups.len(), 2);
    }
}
