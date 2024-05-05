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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Acted event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ActionModuleWhitelisted event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("BaseInitialized event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Blocked event: {:?}", result);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn collect_nftdeployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTDeployed(
        CollectNFTDeployedEvent::new(
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("CollectNFTDeployed event: {:?}", result);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn collect_nfttransferred_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::CollectNFTTransferred(
        CollectNFTTransferredEvent::new(
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("CollectNFTTransferred event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Collected event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("CommentCreated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("DelegatedExecutorsConfigApplied event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("DelegatedExecutorsConfigChanged event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("EmergencyAdminSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("FollowModuleSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("FollowModuleWhitelisted event: {:?}", result);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn follow_nftdeployed_handler(registry: &mut EventCallbackRegistry) {
    LensHubEventType::FollowNFTDeployed(
        FollowNFTDeployedEvent::new(
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("FollowNFTDeployed event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Followed event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("GovernanceSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("MirrorCreated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsCurrencyWhitelisted event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsGovernanceSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsTreasuryFeeSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ModuleGlobalsTreasurySet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("PostCreated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ProfileCreated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ProfileCreatorWhitelisted event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ProfileMetadataSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("QuoteCreated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("ReferenceModuleWhitelisted event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("StateSet event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("TokenGuardianStateChanged event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Unblocked event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("Unfollowed event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("NonceUpdated event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("LensUpgradeVersion event: {:?}", result);
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
            Arc::new(|result, context| {
                Box::pin(async move {
                    println!("CollectedLegacy event: {:?}", result);
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

    collect_nftdeployed_handler(registry).await;

    collect_nfttransferred_handler(registry).await;

    collected_handler(registry).await;

    comment_created_handler(registry).await;

    delegated_executors_config_applied_handler(registry).await;

    delegated_executors_config_changed_handler(registry).await;

    emergency_admin_set_handler(registry).await;

    follow_module_set_handler(registry).await;

    follow_module_whitelisted_handler(registry).await;

    follow_nftdeployed_handler(registry).await;

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
