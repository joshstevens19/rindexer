#![allow(non_snake_case)]
use super::super::super::typings::rindexer_playground::events::playground_types::{
    BasicTypesEvent, BytesTypesEvent, CAPITALIZEDEvent, IrregularWidthSignedIntegersEvent,
    IrregularWidthUnsignedIntegersEvent, PlaygroundTypesEventType, RegularWidthSignedIntegersEvent,
    RegularWidthUnsignedIntegersEvent, TupleTypesEvent, Under_ScoreEvent, no_extensions,
};
use alloy::primitives::{I256, U64, U256};
use rindexer::{
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
};
use std::path::PathBuf;
use std::sync::Arc;

async fn basic_types_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = BasicTypesEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.aBool.to_string(),
                    result.event_data.simpleAddress.to_string(),
                    result.event_data.simpleString.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Bool(result.event_data.aBool),
                    EthereumSqlTypeWrapper::Address(result.event_data.simpleAddress),
                    EthereumSqlTypeWrapper::String(result.event_data.simpleString.clone()),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BasicTypes inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "a_bool".to_string(),
                "simple_address".to_string(),
                "simple_string".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.basic_types",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BasicTypes inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.basic_types",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BasicTypes inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::BasicTypes - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
    .await;

    PlaygroundTypesEventType::BasicTypes(handler).register(manifest_path, registry).await;
}

async fn tuple_types_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = TupleTypesEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result
                        .event_data
                        .array
                        .iter()
                        .cloned()
                        .map(|v| v.address)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .array
                        .iter()
                        .cloned()
                        .map(|v| v.string)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .array
                        .iter()
                        .cloned()
                        .map(|v| v.fixedBytes)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .array
                        .iter()
                        .cloned()
                        .map(|v| v.dynamicBytes)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .nestedArray
                        .iter()
                        .cloned()
                        .map(|v| v.ruleParam)
                        .map(|v| v.key)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .nestedArray
                        .iter()
                        .cloned()
                        .map(|v| v.ruleParam)
                        .flat_map(|v| v.value)
                        .map(|v| v.key)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result
                        .event_data
                        .nestedArray
                        .iter()
                        .cloned()
                        .map(|v| v.ruleParam)
                        .flat_map(|v| v.value)
                        .map(|v| v.value)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::VecAddress(
                        result
                            .event_data
                            .array
                            .iter()
                            .cloned()
                            .map(|v| v.address)
                            .map(|item| item)
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecString(
                        result
                            .event_data
                            .array
                            .iter()
                            .cloned()
                            .map(|v| v.string)
                            .map(|item| item.clone())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecBytes(
                        result
                            .event_data
                            .array
                            .iter()
                            .cloned()
                            .map(|v| v.fixedBytes)
                            .map(|item| item.into())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecBytes(
                        result
                            .event_data
                            .array
                            .iter()
                            .cloned()
                            .map(|v| v.dynamicBytes)
                            .map(|item| item.clone())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecBytes(
                        result
                            .event_data
                            .nestedArray
                            .iter()
                            .cloned()
                            .map(|v| v.ruleParam)
                            .map(|v| v.key)
                            .map(|item| item.into())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecBytes(
                        result
                            .event_data
                            .nestedArray
                            .iter()
                            .cloned()
                            .map(|v| v.ruleParam)
                            .flat_map(|v| v.value)
                            .map(|v| v.key)
                            .map(|item| item.into())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::VecBytes(
                        result
                            .event_data
                            .nestedArray
                            .iter()
                            .cloned()
                            .map(|v| v.ruleParam)
                            .flat_map(|v| v.value)
                            .map(|v| v.value)
                            .map(|item| item.clone())
                            .collect::<Vec<_>>(),
                    ),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::TupleTypes inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "array_address".to_string(),
                "array_string".to_string(),
                "array_fixed_bytes".to_string(),
                "array_dynamic_bytes".to_string(),
                "nested_array_rule_param_key".to_string(),
                "nested_array_rule_param_value_key".to_string(),
                "nested_array_rule_param_value_value".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.tuple_types",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::TupleTypes inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.tuple_types",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::TupleTypes inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::TupleTypes - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
    .await;

    PlaygroundTypesEventType::TupleTypes(handler).register(manifest_path, registry).await;
}

async fn bytes_types_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = BytesTypesEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.aByte1.to_string(),
                    result.event_data.aByte4.to_string(),
                    result.event_data.aByte8.to_string(),
                    result.event_data.aByte16.to_string(),
                    result.event_data.aByte32.to_string(),
                    result.event_data.dynamicBytes.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.aByte1.into()),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.aByte4.into()),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.aByte8.into()),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.aByte16.into()),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.aByte32.into()),
                    EthereumSqlTypeWrapper::Bytes(result.event_data.dynamicBytes.clone()),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BytesTypes inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "a_byte_1".to_string(),
                "a_byte_4".to_string(),
                "a_byte_8".to_string(),
                "a_byte_16".to_string(),
                "a_byte_32".to_string(),
                "dynamic_bytes".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.bytes_types",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BytesTypes inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.bytes_types",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::BytesTypes inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::BytesTypes - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
    .await;

    PlaygroundTypesEventType::BytesTypes(handler).register(manifest_path, registry).await;
}

async fn regular_width_signed_integers_handler(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    let handler = RegularWidthSignedIntegersEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.i8.to_string(),
                    result.event_data.i16.to_string(),
                    result.event_data.i32.to_string(),
                    result.event_data.i64.to_string(),
                    result.event_data.i128.to_string(),
                    result.event_data.i256.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::I8(result.event_data.i8),
                    EthereumSqlTypeWrapper::I16(result.event_data.i16),
                    EthereumSqlTypeWrapper::I32(result.event_data.i32),
                    EthereumSqlTypeWrapper::I64(result.event_data.i64),
                    EthereumSqlTypeWrapper::I128(result.event_data.i128),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i256)),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!("PlaygroundTypesEventType::RegularWidthSignedIntegers inserting csv data: {:?}", e);
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "i_8".to_string(),
                "i_16".to_string(),
                "i_32".to_string(),
                "i_64".to_string(),
                "i_128".to_string(),
                "i_256".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.regular_width_signed_integers",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::RegularWidthSignedIntegers inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.regular_width_signed_integers",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::RegularWidthSignedIntegers inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::RegularWidthSignedIntegers - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::RegularWidthSignedIntegers(handler)
        .register(manifest_path, registry)
        .await;
}

async fn regular_width_unsigned_integers_handler(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    let handler = RegularWidthUnsignedIntegersEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.u8.to_string(),
                    result.event_data.u16.to_string(),
                    result.event_data.u32.to_string(),
                    result.event_data.u64.to_string(),
                    result.event_data.u128.to_string(),
                    result.event_data.u256.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::U8(result.event_data.u8),
                    EthereumSqlTypeWrapper::U16(result.event_data.u16),
                    EthereumSqlTypeWrapper::U32(result.event_data.u32),
                    EthereumSqlTypeWrapper::U64(result.event_data.u64),
                    EthereumSqlTypeWrapper::U128(result.event_data.u128),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u256)),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::RegularWidthUnsignedIntegers inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "u_8".to_string(),
                "u_16".to_string(),
                "u_32".to_string(),
                "u_64".to_string(),
                "u_128".to_string(),
                "u_256".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.regular_width_unsigned_integers",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::RegularWidthUnsignedIntegers inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.regular_width_unsigned_integers",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::RegularWidthUnsignedIntegers inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::RegularWidthUnsignedIntegers - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::RegularWidthUnsignedIntegers(handler)
        .register(manifest_path, registry)
        .await;
}

async fn irregular_width_signed_integers_handler(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    let handler = IrregularWidthSignedIntegersEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.i24.to_string(),
                    result.event_data.i40.to_string(),
                    result.event_data.i48.to_string(),
                    result.event_data.i56.to_string(),
                    result.event_data.i72.to_string(),
                    result.event_data.i80.to_string(),
                    result.event_data.i88.to_string(),
                    result.event_data.i96.to_string(),
                    result.event_data.i104.to_string(),
                    result.event_data.i112.to_string(),
                    result.event_data.i120.to_string(),
                    result.event_data.i136.to_string(),
                    result.event_data.i144.to_string(),
                    result.event_data.i152.to_string(),
                    result.event_data.i160.to_string(),
                    result.event_data.i168.to_string(),
                    result.event_data.i176.to_string(),
                    result.event_data.i184.to_string(),
                    result.event_data.i192.to_string(),
                    result.event_data.i200.to_string(),
                    result.event_data.i208.to_string(),
                    result.event_data.i216.to_string(),
                    result.event_data.i224.to_string(),
                    result.event_data.i232.to_string(),
                    result.event_data.i240.to_string(),
                    result.event_data.i248.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::I32(result.event_data.i24.unchecked_into()),
                    EthereumSqlTypeWrapper::I64(result.event_data.i40.unchecked_into()),
                    EthereumSqlTypeWrapper::I64(result.event_data.i48.unchecked_into()),
                    EthereumSqlTypeWrapper::I64(result.event_data.i56.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i72.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i80.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i88.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i96.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i104.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i112.unchecked_into()),
                    EthereumSqlTypeWrapper::I128(result.event_data.i120.unchecked_into()),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i136)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i144)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i152)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i160)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i168)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i176)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i184)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i192)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i200)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i208)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i216)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i224)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i232)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i240)),
                    EthereumSqlTypeWrapper::I256(I256::from(result.event_data.i248)),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthSignedIntegers inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "i_24".to_string(),
                "i_40".to_string(),
                "i_48".to_string(),
                "i_56".to_string(),
                "i_72".to_string(),
                "i_80".to_string(),
                "i_88".to_string(),
                "i_96".to_string(),
                "i_104".to_string(),
                "i_112".to_string(),
                "i_120".to_string(),
                "i_136".to_string(),
                "i_144".to_string(),
                "i_152".to_string(),
                "i_160".to_string(),
                "i_168".to_string(),
                "i_176".to_string(),
                "i_184".to_string(),
                "i_192".to_string(),
                "i_200".to_string(),
                "i_208".to_string(),
                "i_216".to_string(),
                "i_224".to_string(),
                "i_232".to_string(),
                "i_240".to_string(),
                "i_248".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.irregular_width_signed_integers",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthSignedIntegers inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.irregular_width_signed_integers",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthSignedIntegers inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::IrregularWidthSignedIntegers - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::IrregularWidthSignedIntegers(handler)
        .register(manifest_path, registry)
        .await;
}

async fn irregular_width_unsigned_integers_handler(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    let handler = IrregularWidthUnsignedIntegersEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.u24.to_string(),
                    result.event_data.u40.to_string(),
                    result.event_data.u48.to_string(),
                    result.event_data.u56.to_string(),
                    result.event_data.u72.to_string(),
                    result.event_data.u80.to_string(),
                    result.event_data.u88.to_string(),
                    result.event_data.u96.to_string(),
                    result.event_data.u104.to_string(),
                    result.event_data.u112.to_string(),
                    result.event_data.u120.to_string(),
                    result.event_data.u136.to_string(),
                    result.event_data.u144.to_string(),
                    result.event_data.u152.to_string(),
                    result.event_data.u160.to_string(),
                    result.event_data.u168.to_string(),
                    result.event_data.u176.to_string(),
                    result.event_data.u184.to_string(),
                    result.event_data.u192.to_string(),
                    result.event_data.u200.to_string(),
                    result.event_data.u208.to_string(),
                    result.event_data.u216.to_string(),
                    result.event_data.u224.to_string(),
                    result.event_data.u232.to_string(),
                    result.event_data.u240.to_string(),
                    result.event_data.u248.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::U32(result.event_data.u24.to()),
                    EthereumSqlTypeWrapper::U64(result.event_data.u40.to()),
                    EthereumSqlTypeWrapper::U64(result.event_data.u48.to()),
                    EthereumSqlTypeWrapper::U64(result.event_data.u56.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u72.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u80.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u88.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u96.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u104.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u112.to()),
                    EthereumSqlTypeWrapper::U128(result.event_data.u120.to()),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u136)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u144)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u152)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u160)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u168)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u176)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u184)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u192)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u200)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u208)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u216)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u224)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u232)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u240)),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.u248)),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthUnsignedIntegers inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "u_24".to_string(),
                "u_40".to_string(),
                "u_48".to_string(),
                "u_56".to_string(),
                "u_72".to_string(),
                "u_80".to_string(),
                "u_88".to_string(),
                "u_96".to_string(),
                "u_104".to_string(),
                "u_112".to_string(),
                "u_120".to_string(),
                "u_136".to_string(),
                "u_144".to_string(),
                "u_152".to_string(),
                "u_160".to_string(),
                "u_168".to_string(),
                "u_176".to_string(),
                "u_184".to_string(),
                "u_192".to_string(),
                "u_200".to_string(),
                "u_208".to_string(),
                "u_216".to_string(),
                "u_224".to_string(),
                "u_232".to_string(),
                "u_240".to_string(),
                "u_248".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.irregular_width_unsigned_integers",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthUnsignedIntegers inserting bulk data via COPY: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert(
                        "rindexer_playground_playground_types.irregular_width_unsigned_integers",
                        &rows,
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!(
                        "PlaygroundTypesEventType::IrregularWidthUnsignedIntegers inserting bulk data via INSERT: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            rindexer_info!(
                "PlaygroundTypes::IrregularWidthUnsignedIntegers - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::IrregularWidthUnsignedIntegers(handler)
        .register(manifest_path, registry)
        .await;
}

async fn under__score_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = Under_ScoreEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.foo.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Address(result.event_data.foo),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!("PlaygroundTypesEventType::Under_Score inserting csv data: {:?}", e);
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "foo".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.under__score",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!("PlaygroundTypesEventType::Under_Score inserting bulk data via COPY: {:?}", e);
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert("rindexer_playground_playground_types.under__score", &rows, &postgres_bulk_data)
                    .await;

                if let Err(e) = result {
                    rindexer_error!("PlaygroundTypesEventType::Under_Score inserting bulk data via INSERT: {:?}", e);
                    return Err(e.to_string());
                }
            }

            rindexer_info!("PlaygroundTypes::Under_Score - {} - {} events", "INDEXED".green(), results.len(),);

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::Under_Score(handler).register(manifest_path, registry).await;
}

async fn capitalized_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = CAPITALIZEDEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.foo.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Address(result.event_data.foo),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!("PlaygroundTypesEventType::CAPITALIZED inserting csv data: {:?}", e);
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "foo".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_playground_types.capitalized",
                        &rows,
                        &postgres_bulk_data
                            .first()
                            .ok_or("No first element in bulk data, impossible")?
                            .iter()
                            .map(|param| param.to_type())
                            .collect::<Vec<PgType>>(),
                        &postgres_bulk_data,
                    )
                    .await;

                if let Err(e) = result {
                    rindexer_error!("PlaygroundTypesEventType::CAPITALIZED inserting bulk data via COPY: {:?}", e);
                    return Err(e.to_string());
                }
            } else {
                let result = context
                    .database
                    .bulk_insert("rindexer_playground_playground_types.capitalized", &rows, &postgres_bulk_data)
                    .await;

                if let Err(e) = result {
                    rindexer_error!("PlaygroundTypesEventType::CAPITALIZED inserting bulk data via INSERT: {:?}", e);
                    return Err(e.to_string());
                }
            }

            rindexer_info!("PlaygroundTypes::CAPITALIZED - {} - {} events", "INDEXED".green(), results.len(),);

            Ok(())
        },
        no_extensions(),
    )
        .await;

    PlaygroundTypesEventType::CAPITALIZED(handler).register(manifest_path, registry).await;
}
pub async fn playground_types_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    basic_types_handler(manifest_path, registry).await;

    tuple_types_handler(manifest_path, registry).await;

    bytes_types_handler(manifest_path, registry).await;

    regular_width_signed_integers_handler(manifest_path, registry).await;

    regular_width_unsigned_integers_handler(manifest_path, registry).await;

    irregular_width_signed_integers_handler(manifest_path, registry).await;

    irregular_width_unsigned_integers_handler(manifest_path, registry).await;

    under__score_handler(manifest_path, registry).await;

    capitalized_handler(manifest_path, registry).await;
}
