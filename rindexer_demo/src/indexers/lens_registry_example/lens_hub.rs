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
use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;
use std::sync::Arc;

async fn acted_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::Acted(
        ActedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Acted event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ActionModuleWhitelisted event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("BaseInitialized event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Blocked event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn collect_n_f_t_deployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTDeployed(
        CollectNFTDeployedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("CollectNFTDeployed event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn collect_n_f_t_transferred_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTTransferred(
        CollectNFTTransferredEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("CollectNFTTransferred event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Collected event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("CommentCreated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("DelegatedExecutorsConfigApplied event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("DelegatedExecutorsConfigChanged event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("EmergencyAdminSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("FollowModuleSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("FollowModuleWhitelisted event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn follow_n_f_t_deployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::FollowNFTDeployed(
        FollowNFTDeployedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("FollowNFTDeployed event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Followed event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("GovernanceSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("MirrorCreated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsCurrencyWhitelisted event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsGovernanceSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsTreasuryFeeSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsTreasurySet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("PostCreated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ProfileCreated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ProfileCreatorWhitelisted event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ProfileMetadataSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("QuoteCreated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("ReferenceModuleWhitelisted event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("StateSet event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("TokenGuardianStateChanged event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Unblocked event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("Unfollowed event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("NonceUpdated event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("LensUpgradeVersion event: {:?}", data);
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
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("CollectedLegacy event: {:?}", data);
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

    collect_n_f_t_deployed_handler(registry).await;

    collect_n_f_t_transferred_handler(registry).await;

    collected_handler(registry).await;

    comment_created_handler(registry).await;

    delegated_executors_config_applied_handler(registry).await;

    delegated_executors_config_changed_handler(registry).await;

    emergency_admin_set_handler(registry).await;

    follow_module_set_handler(registry).await;

    follow_module_whitelisted_handler(registry).await;

    follow_n_f_t_deployed_handler(registry).await;

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
