use alloy::json_abi::Param;
use alloy::{
    dyn_abi::DynSolValue,
    primitives::{Address, BlockHash, TxHash, U256, U64},
};
use serde::{Deserialize, Serialize};

/// Metadata inside a log
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMeta {
    /// Address from which this log originated
    pub address: Address,

    /// The block in which the log was emitted
    pub block_number: U64,

    /// The block hash in which the log was emitted
    pub block_hash: BlockHash,

    /// The transaction hash in which the log was emitted
    pub transaction_hash: TxHash,

    /// Transactions index position log was created from
    pub transaction_index: U64,

    /// Log index position in the block
    pub log_index: U256,
}

/// Decoded log param.
#[derive(Debug, PartialEq, Clone)]
pub struct LogParam {
    /// Decoded log name.
    pub name: String,
    /// Decoded log value.
    pub value: DynSolValue,

    /// If the parameter is a compound type (a struct or tuple), a list of the
    /// parameter's components, in order. Empty otherwise
    pub components: Vec<Param>,
}

impl LogParam {
    pub fn new(name: String, value: DynSolValue) -> Self {
        Self { name, value, components: vec![] }
    }

    pub fn get_param_value(&self, name: &str) -> Option<DynSolValue> {
        if self.components.is_empty() {
            return None;
        }

        let mut current_component = self.components.clone();
        let mut current_value = self.value.clone();

        for part in name.split(".") {
            match current_component.iter().enumerate().find(|(_, param)| param.name == part) {
                Some((idx, value)) => {
                    current_component = value.components.clone();
                    current_value = current_value
                        .as_fixed_seq()
                        .expect("Must be a complex type")
                        .get(idx)
                        .expect("Complex type value must be present")
                        .clone();
                }
                None => return None,
            }
        }

        Some(current_value)
    }
}

/// Decoded log.
#[derive(Debug, PartialEq, Clone)]
pub struct ParsedLog {
    /// Log params.
    pub params: Vec<LogParam>,
}

impl ParsedLog {
    /// Extracts param by name. Supports deep paths like `foo.bar.baz`.
    pub fn get_param_value(&self, name: &str) -> Option<DynSolValue> {
        match name.split_once('.') {
            Some((root, rest)) if !rest.is_empty() => self
                .params
                .iter()
                .find(|param| param.name == *root)
                .and_then(|param| param.get_param_value(rest)),
            _ => {
                self.params.iter().find(|param| param.name == name).map(|param| param.value.clone())
            }
        }
    }
}
