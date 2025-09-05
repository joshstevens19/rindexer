use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    database::postgres::relationship::Relationship,
    event::{config::EventProcessingConfig, contract_setup::ContractEventMapping},
    manifest::{contract::DependencyEventTree, core::Manifest},
};

#[derive(Debug, Clone)]
pub struct EventsDependencyTree {
    pub contract_events: Vec<ContractEventMapping>,
    pub then: Box<Option<Arc<EventsDependencyTree>>>,
}

impl EventsDependencyTree {
    pub fn new(events: Vec<ContractEventMapping>) -> Self {
        EventsDependencyTree { contract_events: events, then: Box::new(None) }
    }

    pub fn add_then(&mut self, tree: EventsDependencyTree) {
        self.then = Box::new(Some(Arc::new(tree)));
    }
}

impl EventsDependencyTree {
    pub fn from_dependency_event_tree(event_tree: &DependencyEventTree) -> Self {
        Self {
            contract_events: event_tree.contract_events.clone(),
            then: match event_tree.then.clone() {
                Some(children) => Box::new(Some(Arc::new(
                    EventsDependencyTree::from_dependency_event_tree(&children),
                ))),
                _ => Box::new(None),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventDependencies {
    pub tree: Arc<EventsDependencyTree>,
    pub dependency_events: Vec<ContractEventMapping>,
}

impl EventDependencies {
    pub fn has_dependency(&self, contract_event: &ContractEventMapping) -> bool {
        self.dependency_events.contains(contract_event)
    }
}

#[derive(Debug, Clone)]
pub struct ContractEventDependencies {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
}

#[derive(thiserror::Error, Debug)]
pub enum ContractEventDependenciesMapFromRelationshipsError {
    #[error("Cross contract relationships are need manually mapping in the dependency_events, https://rindexer.xyz/docs/start-building/yaml-config/contracts#dependency_events")]
    CrossContractRelationshipsNotDefinedInDependencyEvents,
}

impl ContractEventDependencies {
    pub fn map_from_relationships(
        relationships: &[Relationship],
    ) -> Result<Vec<ContractEventDependencies>, ContractEventDependenciesMapFromRelationshipsError>
    {
        if Relationship::has_cross_contract_dependency(relationships) {
            return Err(ContractEventDependenciesMapFromRelationshipsError::CrossContractRelationshipsNotDefinedInDependencyEvents);
        }

        Ok(ContractEventDependencies::map_all_dependencies(relationships))
    }

    fn map_all_dependencies(relationships: &[Relationship]) -> Vec<ContractEventDependencies> {
        let relationships_map =
            ContractEventDependencies::generate_relationships_map(relationships);
        let mut result_map = HashMap::new();
        let mut visited = HashSet::new();

        for event in relationships_map.keys() {
            let tree = ContractEventDependencies::build_dependency_tree(
                event,
                &relationships_map,
                &mut visited,
            );
            let dependency_events = ContractEventDependencies::collect_dependency_events(&tree);

            result_map
                .entry(event.contract_name.clone())
                .and_modify(|e: &mut EventDependencies| {
                    e.tree = Arc::new(ContractEventDependencies::merge_trees(&e.tree, &tree));
                    e.dependency_events.extend(dependency_events.clone());
                })
                .or_insert(EventDependencies { tree: Arc::clone(&tree), dependency_events });
        }

        result_map
            .into_iter()
            .map(|(contract_name, event_dependencies)| ContractEventDependencies {
                contract_name,
                event_dependencies,
            })
            .collect()
    }

    fn merge_trees(
        tree1: &EventsDependencyTree,
        tree2: &EventsDependencyTree,
    ) -> EventsDependencyTree {
        let mut contract_events = tree1.contract_events.clone();
        contract_events.extend(tree2.contract_events.clone());
        contract_events.sort_by(|a, b| a.event_name.cmp(&b.event_name));
        contract_events.dedup();

        EventsDependencyTree {
            contract_events,
            then: if tree1.then.is_none() && tree2.then.is_none() {
                Box::new(None)
            } else {
                Box::new(Some(Arc::new(ContractEventDependencies::merge_trees(
                    tree1.then.as_ref().as_ref().unwrap_or(&Arc::new(EventsDependencyTree {
                        contract_events: vec![],
                        then: Box::new(None),
                    })),
                    tree2.then.as_ref().as_ref().unwrap_or(&Arc::new(EventsDependencyTree {
                        contract_events: vec![],
                        then: Box::new(None),
                    })),
                ))))
            },
        }
    }

    fn build_dependency_tree(
        event: &ContractEventMapping,
        relationships_map: &HashMap<ContractEventMapping, Vec<ContractEventMapping>>,
        visited: &mut HashSet<ContractEventMapping>,
    ) -> Arc<EventsDependencyTree> {
        if visited.contains(event) {
            return Arc::new(EventsDependencyTree {
                contract_events: vec![],
                then: Box::new(None),
            });
        }

        visited.insert(event.clone());

        let contract_events = vec![event.clone()];
        let mut next_tree: Option<Arc<EventsDependencyTree>> = None;

        if let Some(linked_events) = relationships_map.get(event) {
            for linked_event in linked_events {
                let tree = ContractEventDependencies::build_dependency_tree(
                    linked_event,
                    relationships_map,
                    visited,
                );
                match next_tree {
                    None => {
                        next_tree = Some(tree);
                    }
                    Some(next_tree_value) => {
                        next_tree = Some(Arc::new(ContractEventDependencies::merge_trees(
                            &next_tree_value,
                            &tree,
                        )));
                    }
                }
            }
        }

        Arc::new(EventsDependencyTree { contract_events, then: Box::new(next_tree) })
    }

    fn generate_relationships_map(
        relationships: &[Relationship],
    ) -> HashMap<ContractEventMapping, Vec<ContractEventMapping>> {
        let mut relationships_map = HashMap::new();

        for relationship in relationships {
            let event = ContractEventMapping {
                contract_name: relationship.contract_name.clone(),
                event_name: relationship.event.clone(),
            };

            let linked_event = ContractEventMapping {
                contract_name: relationship.linked_to.contract_name.clone(),
                event_name: relationship.linked_to.event.clone(),
            };

            relationships_map.entry(linked_event).or_insert_with(Vec::new).push(event);
        }

        relationships_map
    }

    fn collect_dependency_events(tree: &EventsDependencyTree) -> Vec<ContractEventMapping> {
        let mut events = tree.contract_events.clone();
        if let Some(ref then_tree) = *tree.then {
            events.extend(ContractEventDependencies::collect_dependency_events(then_tree));
        }
        events
    }
}

#[derive(Debug)]
pub struct DependencyStatus {
    pub has_dependency_in_own_contract: bool,
    pub dependencies_in_other_contracts: Vec<String>,
}

impl DependencyStatus {
    pub fn has_dependency_in_other_contracts_multiple_times(&self) -> bool {
        self.dependencies_in_other_contracts.len() > 1
    }

    pub fn has_dependencies(&self) -> bool {
        self.has_dependency_in_own_contract || !self.dependencies_in_other_contracts.is_empty()
    }

    pub fn get_first_dependencies_in_other_contracts(&self) -> Option<String> {
        self.dependencies_in_other_contracts.first().cloned()
    }

    pub fn get_all_dependencies_in_other_contracts(&self) -> &[String] {
        &self.dependencies_in_other_contracts
    }
}

impl ContractEventDependencies {
    pub fn parse(manifest: &Manifest) -> Vec<ContractEventDependencies> {
        let mut dependencies: Vec<ContractEventDependencies> = vec![];
        for contract in &manifest.contracts {
            let contract_dependencies_tree = contract
                .dependency_events
                .clone()
                .map(|dependency| contract.convert_dependency_event_tree_yaml(dependency));

            let factories =
                contract.details.iter().filter_map(|d| d.factory.clone()).collect::<Vec<_>>();
            let factory_dependency = factories.first().cloned();

            match factory_dependency {
                // When factory filter is set we rewrite dependency settings to put factory event first
                Some(factory) => {
                    if !factories.iter().all(|f| {
                        f.name == factory.name
                            && f.event_name == factory.event_name
                            && f.input_name == factory.input_name
                    }) {
                        panic!("Contract using factory filter must use same factory across all networks. Please raise issue in github if you need different factories across networks");
                    }

                    let dependency_event_tree = contract_dependencies_tree.unwrap_or_else(|| {
                        let events = contract
                            .include_events
                            .as_ref()
                            .expect("Contract using factory filter must specify `include_events`.");

                        let dependency_event_tree = DependencyEventTree {
                            contract_events: events
                                .iter()
                                .map(|event| ContractEventMapping {
                                    contract_name: contract.name.clone(),
                                    event_name: event.name.clone(),
                                })
                                .collect(),
                            then: None,
                        };

                        dependency_event_tree
                    });

                    let factory_dependency_event_tree = DependencyEventTree {
                        contract_events: vec![ContractEventMapping {
                            contract_name: factory.name,
                            event_name: factory.event_name,
                        }],
                        then: Some(Box::from(dependency_event_tree)),
                    };

                    let factory_dependency_tree = EventsDependencyTree::from_dependency_event_tree(
                        &factory_dependency_event_tree,
                    );

                    dependencies.push(ContractEventDependencies {
                        contract_name: contract.name.clone(),
                        event_dependencies: EventDependencies {
                            tree: Arc::new(factory_dependency_tree),
                            dependency_events: factory_dependency_event_tree
                                .collect_dependency_events(),
                        },
                    });
                }
                None => {
                    if let Some(dependency_event_tree) = contract_dependencies_tree {
                        let dependency_tree = EventsDependencyTree::from_dependency_event_tree(
                            &dependency_event_tree,
                        );

                        dependencies.push(ContractEventDependencies {
                            contract_name: contract.name.clone(),
                            event_dependencies: EventDependencies {
                                tree: Arc::new(dependency_tree),
                                dependency_events: dependency_event_tree
                                    .collect_dependency_events(),
                            },
                        });
                    }
                }
            }
        }

        dependencies
    }

    pub fn dependencies_status(
        contract_name: &str,
        event_name: &str,
        dependencies: &[ContractEventDependencies],
    ) -> DependencyStatus {
        let has_dependency_in_own_contract =
            dependencies.iter().find(|d| d.contract_name == contract_name).is_some_and(|deps| {
                deps.event_dependencies.has_dependency(&ContractEventMapping {
                    contract_name: deps.contract_name.clone(),
                    event_name: event_name.to_string(),
                })
            });

        let dependencies_in_other_contracts: Vec<String> = dependencies
            .iter()
            .filter_map(|d| {
                if d.contract_name != contract_name {
                    let has_dependency =
                        d.event_dependencies.has_dependency(&ContractEventMapping {
                            contract_name: contract_name.to_string(),
                            event_name: event_name.to_string(),
                        });

                    if has_dependency {
                        return Some(d.contract_name.to_string());
                    }

                    // check if it's a filter event
                    let has_dependency =
                        d.event_dependencies.has_dependency(&ContractEventMapping {
                            // TODO - this is a hacky way to check if it's a filter event
                            contract_name: contract_name.to_string().replace("Filter", ""),
                            event_name: event_name.to_string(),
                        });
                    if has_dependency {
                        return Some(d.contract_name.to_string());
                    }
                }
                None
            })
            .collect();

        DependencyStatus { has_dependency_in_own_contract, dependencies_in_other_contracts }
    }
}

pub struct ContractEventsDependenciesConfig {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
    pub events_config: Vec<Arc<EventProcessingConfig>>,
}

impl ContractEventsDependenciesConfig {
    fn add_event_config(&mut self, config: Arc<EventProcessingConfig>) {
        self.events_config.push(config);
    }

    pub fn add_to_event_or_new_entry(
        dependency_event_processing_configs: &mut Vec<ContractEventsDependenciesConfig>,
        event_processing_config: Arc<EventProcessingConfig>,
        dependencies: &[ContractEventDependencies],
    ) {
        match dependency_event_processing_configs
            .iter_mut()
            .find(|c| c.contract_name == event_processing_config.contract_name())
        {
            Some(contract_events_config) => {
                contract_events_config.add_event_config(event_processing_config)
            }
            None => {
                dependency_event_processing_configs.push(ContractEventsDependenciesConfig {
                    contract_name: event_processing_config.contract_name().clone(),
                    event_dependencies: dependencies
                        .iter()
                        .find(|d| d.contract_name == event_processing_config.contract_name())
                        .expect("Failed to find contract dependencies")
                        .event_dependencies
                        .clone(),
                    events_config: vec![event_processing_config],
                });
            }
        }
    }

    pub fn add_to_event_or_panic(
        contract_name: &str,
        dependency_event_processing_configs: &mut [ContractEventsDependenciesConfig],
        event_processing_config: Arc<EventProcessingConfig>,
    ) {
        match dependency_event_processing_configs
            .iter_mut()
            .find(|c| c.contract_name == contract_name)
        {
            Some(contract_events_config) => {
                contract_events_config.add_event_config(event_processing_config)
            }
            None => {
                panic!("Contract events config not found for {} dependency event processing config make sure it registered - trying to add to it - contract {} - event {}",
                       contract_name,
                       event_processing_config.contract_name(),
                       event_processing_config.event_name()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::contract_setup::ContractEventMapping;
    use std::sync::Arc;

    #[test]
    fn test_single_cross_contract_dependency() {
        // This should work - single dependency across contracts
        let dependencies = vec![
            ContractEventDependencies {
                contract_name: "ContractA".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }])),
                    dependency_events: vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }],
                },
            },
            ContractEventDependencies {
                contract_name: "ContractB".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![
                        ContractEventMapping {
                            contract_name: "ContractB".to_string(),
                            event_name: "EventB".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ])),
                    dependency_events: vec![
                        ContractEventMapping {
                            contract_name: "ContractB".to_string(),
                            event_name: "EventB".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ],
                },
            },
        ];

        let status =
            ContractEventDependencies::dependencies_status("ContractA", "EventA", &dependencies);

        assert!(status.has_dependency_in_own_contract);
        assert_eq!(status.dependencies_in_other_contracts.len(), 1);
        assert_eq!(status.dependencies_in_other_contracts[0], "ContractB");
        assert!(!status.has_dependency_in_other_contracts_multiple_times());
    }

    #[test]
    fn test_multiple_cross_contract_dependencies_same_event() {
        // This tests the scenario where multiple contracts depend on the same event
        // Scenario: EventA from ContractA is a dependency for both ContractB and ContractC
        let dependencies = vec![
            ContractEventDependencies {
                contract_name: "ContractA".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }])),
                    dependency_events: vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }],
                },
            },
            ContractEventDependencies {
                contract_name: "ContractB".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![
                        ContractEventMapping {
                            contract_name: "ContractB".to_string(),
                            event_name: "EventB".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ])),
                    dependency_events: vec![
                        ContractEventMapping {
                            contract_name: "ContractB".to_string(),
                            event_name: "EventB".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ],
                },
            },
            ContractEventDependencies {
                contract_name: "ContractC".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![
                        ContractEventMapping {
                            contract_name: "ContractC".to_string(),
                            event_name: "EventC".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ])),
                    dependency_events: vec![
                        ContractEventMapping {
                            contract_name: "ContractC".to_string(),
                            event_name: "EventC".to_string(),
                        },
                        ContractEventMapping {
                            contract_name: "ContractA".to_string(),
                            event_name: "EventA".to_string(),
                        },
                    ],
                },
            },
        ];

        let status =
            ContractEventDependencies::dependencies_status("ContractA", "EventA", &dependencies);

        assert!(status.has_dependency_in_own_contract);
        assert_eq!(status.dependencies_in_other_contracts.len(), 2);
        assert!(status.dependencies_in_other_contracts.contains(&"ContractB".to_string()));
        assert!(status.dependencies_in_other_contracts.contains(&"ContractC".to_string()));

        // This is now supported - multiple cross-contract dependencies work!
        assert!(status.has_dependency_in_other_contracts_multiple_times());
    }

    #[test]
    fn test_multiple_cross_contract_dependencies_now_supported() {
        // This test verifies that multiple cross-contract dependencies are now supported
        // Previously this would trigger a panic, but now it should work correctly

        let dependencies = vec![
            ContractEventDependencies {
                contract_name: "ContractA".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![])),
                    dependency_events: vec![],
                },
            },
            ContractEventDependencies {
                contract_name: "ContractB".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![])),
                    dependency_events: vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }],
                },
            },
            ContractEventDependencies {
                contract_name: "ContractC".to_string(),
                event_dependencies: EventDependencies {
                    tree: Arc::new(EventsDependencyTree::new(vec![])),
                    dependency_events: vec![ContractEventMapping {
                        contract_name: "ContractA".to_string(),
                        event_name: "EventA".to_string(),
                    }],
                },
            },
        ];

        let status =
            ContractEventDependencies::dependencies_status("ContractA", "EventA", &dependencies);

        // Verify that the multiple cross-contract dependencies are detected correctly
        assert!(status.has_dependency_in_other_contracts_multiple_times());
        assert_eq!(status.get_all_dependencies_in_other_contracts().len(), 2);
        assert!(status
            .get_all_dependencies_in_other_contracts()
            .contains(&"ContractB".to_string()));
        assert!(status
            .get_all_dependencies_in_other_contracts()
            .contains(&"ContractC".to_string()));

        // This should no longer panic - the feature is now supported!
    }

    #[test]
    fn test_get_all_dependencies_in_other_contracts() {
        // Test the new method we added
        let status = DependencyStatus {
            has_dependency_in_own_contract: false,
            dependencies_in_other_contracts: vec![
                "ContractB".to_string(),
                "ContractC".to_string(),
                "ContractD".to_string(),
            ],
        };

        let all_deps = status.get_all_dependencies_in_other_contracts();
        assert_eq!(all_deps.len(), 3);
        assert!(all_deps.contains(&"ContractB".to_string()));
        assert!(all_deps.contains(&"ContractC".to_string()));
        assert!(all_deps.contains(&"ContractD".to_string()));

        assert!(status.has_dependency_in_other_contracts_multiple_times());
        assert_eq!(
            status.get_first_dependencies_in_other_contracts(),
            Some("ContractB".to_string())
        );
    }
}
