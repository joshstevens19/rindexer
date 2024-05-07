use crate::rindexer::lens_registry_example::events::lens_hub::{
    no_extensions, ActedEvent, ActionModuleWhitelistedEvent, BaseInitializedEvent, BlockedEvent,
    CollectNFTDeployedEvent, CollectNFTTransferredEvent, CollectedEvent, CollectedLegacyEvent,
    CommentCreatedEvent, DelegatedExecutorsConfigAppliedEvent,
    DelegatedExecutorsConfigChangedEvent, EmergencyAdminSetEvent, FollowModuleSetEvent,
    FollowModuleWhitelistedEvent, FollowNFTDeployedEvent, FollowedEvent, GovernanceSetEvent,
    LensHubEventType, LensUpgradeVersionEvent, MirrorCreatedEvent,
    ModuleGlobalsCurrencyWhitelistedEvent, ModuleGlobalsGovernanceSetEvent,
    ModuleGlobalsTreasuryFeeSetEvent, ModuleGlobalsTreasurySetEvent, NewEventOptions,
    NonceUpdatedEvent, PostCreatedEvent, ProfileCreatedEvent, ProfileCreatorWhitelistedEvent,
    ProfileMetadataSetEvent, QuoteCreatedEvent, ReferenceModuleWhitelistedEvent, StateSetEvent,
    TokenGuardianStateChangedEvent, UnblockedEvent, UnfollowedEvent,
};
use rindexer_core::{
    generator::event_callback_registry::EventCallbackRegistry, EthereumSqlTypeWrapper,
};
use std::sync::Arc;

async fn acted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Acted(
                    ActedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Acted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.acted (contract_address, \"publication_action_params_publication_acted_profile_id\", \"publication_action_params_publication_acted_id\", \"publication_action_params_actor_profile_id\", \"publication_action_params_referrer_profile_ids\", \"publication_action_params_referrer_pub_ids\", \"publication_action_params_action_module_address\", \"publication_action_params_action_module_data\", \"action_module_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.publication_action_params.publication_acted_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.publication_action_params.publication_acted_id),&EthereumSqlTypeWrapper::U256(&result.event_data.publication_action_params.actor_profile_id),&EthereumSqlTypeWrapper::VecU256(&result.event_data.publication_action_params.referrer_profile_ids),&EthereumSqlTypeWrapper::VecU256(&result.event_data.publication_action_params.referrer_pub_ids),&EthereumSqlTypeWrapper::Address(&result.event_data.publication_action_params.action_module_address),&EthereumSqlTypeWrapper::Bytes(&result.event_data.publication_action_params.action_module_data),&EthereumSqlTypeWrapper::Bytes(&result.event_data.action_module_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn action_module_whitelisted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ActionModuleWhitelisted(
                    ActionModuleWhitelistedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ActionModuleWhitelisted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.action_module_whitelisted (contract_address, \"action_module\", \"id\", \"whitelisted\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.action_module),&EthereumSqlTypeWrapper::U256(&result.event_data.id),&EthereumSqlTypeWrapper::Bool(&result.event_data.whitelisted),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn base_initialized_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::BaseInitialized(
                    BaseInitializedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("BaseInitialized event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.base_initialized (contract_address, \"name\", \"symbol\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::String(&result.event_data.name),&EthereumSqlTypeWrapper::String(&result.event_data.symbol),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn blocked_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Blocked(
                    BlockedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Blocked event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.blocked (contract_address, \"by_profile_id\", \"id_of_profile_blocked\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.by_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.id_of_profile_blocked),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn collect_nft_deployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTDeployed(
                    CollectNFTDeployedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("CollectNFTDeployed event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.collect_nft_deployed (contract_address, \"profile_id\", \"pub_id\", \"collect_nft\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::Address(&result.event_data.collect_nft),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn collect_nft_transferred_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTTransferred(
                    CollectNFTTransferredEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("CollectNFTTransferred event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.collect_nft_transferred (contract_address, \"profile_id\", \"pub_id\", \"collect_nft_id\", \"from\", \"to\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::U256(&result.event_data.collect_nft_id),&EthereumSqlTypeWrapper::Address(&result.event_data.from),&EthereumSqlTypeWrapper::Address(&result.event_data.to),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn collected_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Collected(
                    CollectedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Collected event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.collected (contract_address, \"collected_profile_id\", \"collected_pub_id\", \"collector_profile_id\", \"nft_recipient\", \"collect_action_data\", \"collect_action_result\", \"collect_nft\", \"token_id\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.collected_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.collected_pub_id),&EthereumSqlTypeWrapper::U256(&result.event_data.collector_profile_id),&EthereumSqlTypeWrapper::Address(&result.event_data.nft_recipient),&EthereumSqlTypeWrapper::Bytes(&result.event_data.collect_action_data),&EthereumSqlTypeWrapper::Bytes(&result.event_data.collect_action_result),&EthereumSqlTypeWrapper::Address(&result.event_data.collect_nft),&EthereumSqlTypeWrapper::U256(&result.event_data.token_id),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn comment_created_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CommentCreated(
                    CommentCreatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("CommentCreated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.comment_created (contract_address, \"comment_params_profile_id\", \"comment_params_content_uri\", \"comment_params_pointed_profile_id\", \"comment_params_pointed_pub_id\", \"comment_params_referrer_profile_ids\", \"comment_params_referrer_pub_ids\", \"comment_params_reference_module_data\", \"comment_params_action_modules\", \"comment_params_action_modules_init_datas\", \"comment_params_reference_module\", \"comment_params_reference_module_init_data\", \"pub_id\", \"reference_module_return_data\", \"action_modules_init_return_datas\", \"reference_module_init_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.comment_params.profile_id),&EthereumSqlTypeWrapper::String(&result.event_data.comment_params.content_uri),&EthereumSqlTypeWrapper::U256(&result.event_data.comment_params.pointed_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.comment_params.pointed_pub_id),&EthereumSqlTypeWrapper::VecU256(&result.event_data.comment_params.referrer_profile_ids),&EthereumSqlTypeWrapper::VecU256(&result.event_data.comment_params.referrer_pub_ids),&EthereumSqlTypeWrapper::Bytes(&result.event_data.comment_params.reference_module_data),&EthereumSqlTypeWrapper::VecAddress(&result.event_data.comment_params.action_modules),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.comment_params.action_modules_init_datas),&EthereumSqlTypeWrapper::Address(&result.event_data.comment_params.reference_module),&EthereumSqlTypeWrapper::Bytes(&result.event_data.comment_params.reference_module_init_data),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_return_data),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.action_modules_init_return_datas),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_init_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn delegated_executors_config_applied_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::DelegatedExecutorsConfigApplied(
                    DelegatedExecutorsConfigAppliedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("DelegatedExecutorsConfigApplied event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.delegated_executors_config_applied (contract_address, \"delegator_profile_id\", \"config_number\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.delegator_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.config_number),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn delegated_executors_config_changed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::DelegatedExecutorsConfigChanged(
                    DelegatedExecutorsConfigChangedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                // println!("DelegatedExecutorsConfigChanged event: {:?}", results);
                                for result in results {
                                    println!("DelegatedExecutorsConfigChanged result: {:?}", result);
                                    // context
                                    //     .database
                                    //     .execute("\
                                    //         INSERT INTO lens_registry_example.delegated_executors_config_changed (\
                                    //             contract_address,\
                                    //             \"delegator_profile_id\",\
                                    //             \"config_number\",\
                                    //             \"delegated_executors\",\
                                    //             \"approvals\",\
                                    //             \"timestamp\",\
                                    //             \"tx_hash\",\
                                    //             \"block_number\",\
                                    //             \"block_hash\")\
                                    //         VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                                    //     &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),
                                    //               &EthereumSqlTypeWrapper::U256(&result.event_data.delegator_profile_id),
                                    //               &EthereumSqlTypeWrapper::U256(&result.event_data.config_number),
                                    //               &EthereumSqlTypeWrapper::VecAddress(&result.event_data.delegated_executors),
                                    //               &EthereumSqlTypeWrapper::VecBool(&result.event_data.approvals),
                                    //               &EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),
                                    //               &EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),
                                    //               &EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),
                                    //               &EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                                    //     .await.unwrap();

                                    context
                                        .database
                                        .execute("\
                                            INSERT INTO lens_registry_example.delegated_executors_config_changed (\
                                                contract_address,\
                                                \"delegator_profile_id\",\
                                                \"config_number\",\
                                                \"approvals\",\
                                                \"delegated_executors\",\
                                                \"timestamp\",\
                                                \"tx_hash\",\
                                                \"block_number\",\
                                                \"block_hash\")\
                                            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),
                                                  &EthereumSqlTypeWrapper::U256(&result.event_data.delegator_profile_id),
                                                  &EthereumSqlTypeWrapper::U256(&result.event_data.config_number),
                                                  &EthereumSqlTypeWrapper::VecBool(&result.event_data.approvals),
                                                  &EthereumSqlTypeWrapper::VecAddress(&result.event_data.delegated_executors),
                                                  &EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),
                                                  &EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),
                                                  &EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),
                                                  &EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                                        .await.unwrap();

                                        }
                                })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn emergency_admin_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::EmergencyAdminSet(
                    EmergencyAdminSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("EmergencyAdminSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.emergency_admin_set (contract_address, \"caller\", \"old_emergency_admin\", \"new_emergency_admin\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.caller),&EthereumSqlTypeWrapper::Address(&result.event_data.old_emergency_admin),&EthereumSqlTypeWrapper::Address(&result.event_data.new_emergency_admin),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn follow_module_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::FollowModuleSet(
                    FollowModuleSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("FollowModuleSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.follow_module_set (contract_address, \"profile_id\", \"follow_module\", \"follow_module_init_data\", \"follow_module_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::Address(&result.event_data.follow_module),&EthereumSqlTypeWrapper::Bytes(&result.event_data.follow_module_init_data),&EthereumSqlTypeWrapper::Bytes(&result.event_data.follow_module_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn follow_module_whitelisted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::FollowModuleWhitelisted(
                    FollowModuleWhitelistedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("FollowModuleWhitelisted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.follow_module_whitelisted (contract_address, \"follow_module\", \"whitelisted\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.follow_module),&EthereumSqlTypeWrapper::Bool(&result.event_data.whitelisted),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn follow_nft_deployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::FollowNFTDeployed(
                    FollowNFTDeployedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("FollowNFTDeployed event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.follow_nft_deployed (contract_address, \"profile_id\", \"follow_nft\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::Address(&result.event_data.follow_nft),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn followed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Followed(
                    FollowedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Followed event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.followed (contract_address, \"follower_profile_id\", \"id_of_profile_followed\", \"follow_token_id_assigned\", \"follow_module_data\", \"process_follow_module_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.follower_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.id_of_profile_followed),&EthereumSqlTypeWrapper::U256(&result.event_data.follow_token_id_assigned),&EthereumSqlTypeWrapper::Bytes(&result.event_data.follow_module_data),&EthereumSqlTypeWrapper::Bytes(&result.event_data.process_follow_module_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn governance_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::GovernanceSet(
                    GovernanceSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("GovernanceSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.governance_set (contract_address, \"caller\", \"prev_governance\", \"new_governance\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.caller),&EthereumSqlTypeWrapper::Address(&result.event_data.prev_governance),&EthereumSqlTypeWrapper::Address(&result.event_data.new_governance),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn mirror_created_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::MirrorCreated(
                    MirrorCreatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("MirrorCreated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.mirror_created (contract_address, \"mirror_params_profile_id\", \"mirror_params_metadata_uri\", \"mirror_params_pointed_profile_id\", \"mirror_params_pointed_pub_id\", \"mirror_params_referrer_profile_ids\", \"mirror_params_referrer_pub_ids\", \"mirror_params_reference_module_data\", \"pub_id\", \"reference_module_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.mirror_params.profile_id),&EthereumSqlTypeWrapper::String(&result.event_data.mirror_params.metadata_uri),&EthereumSqlTypeWrapper::U256(&result.event_data.mirror_params.pointed_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.mirror_params.pointed_pub_id),&EthereumSqlTypeWrapper::VecU256(&result.event_data.mirror_params.referrer_profile_ids),&EthereumSqlTypeWrapper::VecU256(&result.event_data.mirror_params.referrer_pub_ids),&EthereumSqlTypeWrapper::Bytes(&result.event_data.mirror_params.reference_module_data),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn module_globals_currency_whitelisted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ModuleGlobalsCurrencyWhitelisted(
                    ModuleGlobalsCurrencyWhitelistedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ModuleGlobalsCurrencyWhitelisted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.module_globals_currency_whitelisted (contract_address, \"currency\", \"prev_whitelisted\", \"whitelisted\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.currency),&EthereumSqlTypeWrapper::Bool(&result.event_data.prev_whitelisted),&EthereumSqlTypeWrapper::Bool(&result.event_data.whitelisted),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn module_globals_governance_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ModuleGlobalsGovernanceSet(
                    ModuleGlobalsGovernanceSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ModuleGlobalsGovernanceSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.module_globals_governance_set (contract_address, \"prev_governance\", \"new_governance\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.prev_governance),&EthereumSqlTypeWrapper::Address(&result.event_data.new_governance),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn module_globals_treasury_fee_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ModuleGlobalsTreasuryFeeSet(
                    ModuleGlobalsTreasuryFeeSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ModuleGlobalsTreasuryFeeSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.module_globals_treasury_fee_set (contract_address, \"prev_treasury_fee\", \"new_treasury_fee\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U16(&result.event_data.prev_treasury_fee),&EthereumSqlTypeWrapper::U16(&result.event_data.new_treasury_fee),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn module_globals_treasury_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ModuleGlobalsTreasurySet(
                    ModuleGlobalsTreasurySetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ModuleGlobalsTreasurySet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.module_globals_treasury_set (contract_address, \"prev_treasury\", \"new_treasury\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.prev_treasury),&EthereumSqlTypeWrapper::Address(&result.event_data.new_treasury),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn post_created_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::PostCreated(
                    PostCreatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("PostCreated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.post_created (contract_address, \"post_params_profile_id\", \"post_params_content_uri\", \"post_params_action_modules\", \"post_params_action_modules_init_datas\", \"post_params_reference_module\", \"post_params_reference_module_init_data\", \"pub_id\", \"action_modules_init_return_datas\", \"reference_module_init_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.post_params.profile_id),&EthereumSqlTypeWrapper::String(&result.event_data.post_params.content_uri),&EthereumSqlTypeWrapper::VecAddress(&result.event_data.post_params.action_modules),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.post_params.action_modules_init_datas),&EthereumSqlTypeWrapper::Address(&result.event_data.post_params.reference_module),&EthereumSqlTypeWrapper::Bytes(&result.event_data.post_params.reference_module_init_data),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.action_modules_init_return_datas),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_init_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn profile_created_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ProfileCreated(
                    ProfileCreatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ProfileCreated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.profile_created (contract_address, \"profile_id\", \"creator\", \"to\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::Address(&result.event_data.creator),&EthereumSqlTypeWrapper::Address(&result.event_data.to),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn profile_creator_whitelisted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ProfileCreatorWhitelisted(
                    ProfileCreatorWhitelistedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ProfileCreatorWhitelisted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.profile_creator_whitelisted (contract_address, \"profile_creator\", \"whitelisted\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.profile_creator),&EthereumSqlTypeWrapper::Bool(&result.event_data.whitelisted),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn profile_metadata_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ProfileMetadataSet(
                    ProfileMetadataSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ProfileMetadataSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.profile_metadata_set (contract_address, \"profile_id\", \"metadata\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.profile_id),&EthereumSqlTypeWrapper::String(&result.event_data.metadata),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn quote_created_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::QuoteCreated(
                    QuoteCreatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("QuoteCreated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.quote_created (contract_address, \"quote_params_profile_id\", \"quote_params_content_uri\", \"quote_params_pointed_profile_id\", \"quote_params_pointed_pub_id\", \"quote_params_referrer_profile_ids\", \"quote_params_referrer_pub_ids\", \"quote_params_reference_module_data\", \"quote_params_action_modules\", \"quote_params_action_modules_init_datas\", \"quote_params_reference_module\", \"quote_params_reference_module_init_data\", \"pub_id\", \"reference_module_return_data\", \"action_modules_init_return_datas\", \"reference_module_init_return_data\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.quote_params.profile_id),&EthereumSqlTypeWrapper::String(&result.event_data.quote_params.content_uri),&EthereumSqlTypeWrapper::U256(&result.event_data.quote_params.pointed_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.quote_params.pointed_pub_id),&EthereumSqlTypeWrapper::VecU256(&result.event_data.quote_params.referrer_profile_ids),&EthereumSqlTypeWrapper::VecU256(&result.event_data.quote_params.referrer_pub_ids),&EthereumSqlTypeWrapper::Bytes(&result.event_data.quote_params.reference_module_data),&EthereumSqlTypeWrapper::VecAddress(&result.event_data.quote_params.action_modules),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.quote_params.action_modules_init_datas),&EthereumSqlTypeWrapper::Address(&result.event_data.quote_params.reference_module),&EthereumSqlTypeWrapper::Bytes(&result.event_data.quote_params.reference_module_init_data),&EthereumSqlTypeWrapper::U256(&result.event_data.pub_id),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_return_data),&EthereumSqlTypeWrapper::VecBytes(&result.event_data.action_modules_init_return_datas),&EthereumSqlTypeWrapper::Bytes(&result.event_data.reference_module_init_return_data),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
          })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn reference_module_whitelisted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::ReferenceModuleWhitelisted(
                    ReferenceModuleWhitelistedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("ReferenceModuleWhitelisted event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.reference_module_whitelisted (contract_address, \"reference_module\", \"whitelisted\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.reference_module),&EthereumSqlTypeWrapper::Bool(&result.event_data.whitelisted),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn state_set_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::StateSet(
                    StateSetEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("StateSet event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.state_set (contract_address, \"caller\", \"prev_state\", \"new_state\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.caller),&EthereumSqlTypeWrapper::U8(&result.event_data.prev_state),&EthereumSqlTypeWrapper::U8(&result.event_data.new_state),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn token_guardian_state_changed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::TokenGuardianStateChanged(
                    TokenGuardianStateChangedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("TokenGuardianStateChanged event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.token_guardian_state_changed (contract_address, \"wallet\", \"enabled\", \"token_guardian_disabling_timestamp\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.wallet),&EthereumSqlTypeWrapper::Bool(&result.event_data.enabled),&EthereumSqlTypeWrapper::U256(&result.event_data.token_guardian_disabling_timestamp),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn unblocked_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Unblocked(
                    UnblockedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Unblocked event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.unblocked (contract_address, \"by_profile_id\", \"id_of_profile_unblocked\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.by_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.id_of_profile_unblocked),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn unfollowed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Unfollowed(
                    UnfollowedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Unfollowed event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.unfollowed (contract_address, \"unfollower_profile_id\", \"id_of_profile_unfollowed\", \"transaction_executor\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.unfollower_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.id_of_profile_unfollowed),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn nonce_updated_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::NonceUpdated(
                    NonceUpdatedEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("NonceUpdated event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.nonce_updated (contract_address, \"signer\", \"nonce\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.signer),&EthereumSqlTypeWrapper::U256(&result.event_data.nonce),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn lens_upgrade_version_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::LensUpgradeVersion(
                    LensUpgradeVersionEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("LensUpgradeVersion event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.lens_upgrade_version (contract_address, \"implementation\", \"version\", \"git_commit\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.implementation),&EthereumSqlTypeWrapper::String(&result.event_data.version),&EthereumSqlTypeWrapper::Bytes(&result.event_data.git_commit.into()),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn collected_legacy_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectedLegacy(
                    CollectedLegacyEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("CollectedLegacy event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.collected_legacy (contract_address, \"publication_collected_profile_id\", \"publication_collected_id\", \"collector_profile_id\", \"transaction_executor\", \"referrer_profile_id\", \"referrer_pub_id\", \"collect_module\", \"collect_module_data\", \"token_id\", \"timestamp\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::U256(&result.event_data.publication_collected_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.publication_collected_id),&EthereumSqlTypeWrapper::U256(&result.event_data.collector_profile_id),&EthereumSqlTypeWrapper::Address(&result.event_data.transaction_executor),&EthereumSqlTypeWrapper::U256(&result.event_data.referrer_profile_id),&EthereumSqlTypeWrapper::U256(&result.event_data.referrer_pub_id),&EthereumSqlTypeWrapper::Address(&result.event_data.collect_module),&EthereumSqlTypeWrapper::Bytes(&result.event_data.collect_module_data),&EthereumSqlTypeWrapper::U256(&result.event_data.token_id),&EthereumSqlTypeWrapper::U256(&result.event_data.timestamp),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}
pub async fn lens_hub_handlers(registry: &mut EventCallbackRegistry) {
    acted_handler(registry).await;

    action_module_whitelisted_handler(registry).await;

    base_initialized_handler(registry).await;

    blocked_handler(registry).await;

    collect_nft_deployed_handler(registry).await;

    collect_nft_transferred_handler(registry).await;

    collected_handler(registry).await;

    comment_created_handler(registry).await;

    delegated_executors_config_applied_handler(registry).await;

    delegated_executors_config_changed_handler(registry).await;

    emergency_admin_set_handler(registry).await;

    follow_module_set_handler(registry).await;

    follow_module_whitelisted_handler(registry).await;

    follow_nft_deployed_handler(registry).await;

    followed_handler(registry).await;

    governance_set_handler(registry).await;

    mirror_created_handler(registry).await;

    module_globals_currency_whitelisted_handler(registry).await;

    module_globals_governance_set_handler(registry).await;

    module_globals_treasury_fee_set_handler(registry).await;

    module_globals_treasury_set_handler(registry).await;

    post_created_handler(registry).await;

    profile_created_handler(registry).await;

    profile_creator_whitelisted_handler(registry).await;

    profile_metadata_set_handler(registry).await;

    quote_created_handler(registry).await;

    reference_module_whitelisted_handler(registry).await;

    state_set_handler(registry).await;

    token_guardian_state_changed_handler(registry).await;

    unblocked_handler(registry).await;

    unfollowed_handler(registry).await;

    nonce_updated_handler(registry).await;

    lens_upgrade_version_handler(registry).await;

    collected_legacy_handler(registry).await;
}
