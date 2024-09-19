pub use rindexer_playground_types_filter_gen::*;
/// This module was auto-generated with ethers-rs Abigen.
/// More information at: <https://github.com/gakonst/ethers-rs>
#[allow(
    clippy::enum_variant_names,
    clippy::too_many_arguments,
    clippy::upper_case_acronyms,
    clippy::type_complexity,
    dead_code,
    non_camel_case_types
)]
pub mod rindexer_playground_types_filter_gen {
    #[allow(deprecated)]
    fn __abi() -> ::ethers::core::abi::Abi {
        ::ethers::core::abi::ethabi::Contract {
            constructor: ::core::option::Option::None,
            functions: ::std::collections::BTreeMap::new(),
            events: ::core::convert::From::from([(
                ::std::borrow::ToOwned::to_owned("Swap"),
                ::std::vec![::ethers::core::abi::ethabi::Event {
                    name: ::std::borrow::ToOwned::to_owned("Swap"),
                    inputs: ::std::vec![
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("sender"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Address,
                            indexed: true,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("recipient"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Address,
                            indexed: true,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("amount0"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(256usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("amount1"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(256usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("sqrtPriceX96"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Uint(160usize,),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("liquidity"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Uint(128usize,),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(24usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick2"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(8usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick3"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(16usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick4"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(32usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick5"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(64usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick6"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(128usize),
                            indexed: false,
                        },
                        ::ethers::core::abi::ethabi::EventParam {
                            name: ::std::borrow::ToOwned::to_owned("tick7"),
                            kind: ::ethers::core::abi::ethabi::ParamType::Int(192usize),
                            indexed: false,
                        },
                    ],
                    anonymous: false,
                },],
            )]),
            errors: ::std::collections::BTreeMap::new(),
            receive: false,
            fallback: false,
        }
    }
    ///The parsed JSON ABI of the contract.
    pub static RINDEXERPLAYGROUNDTYPESFILTERGEN_ABI: ::ethers::contract::Lazy<
        ::ethers::core::abi::Abi,
    > = ::ethers::contract::Lazy::new(__abi);
    pub struct RindexerPlaygroundTypesFilterGen<M>(::ethers::contract::Contract<M>);
    impl<M> ::core::clone::Clone for RindexerPlaygroundTypesFilterGen<M> {
        fn clone(&self) -> Self {
            Self(::core::clone::Clone::clone(&self.0))
        }
    }
    impl<M> ::core::ops::Deref for RindexerPlaygroundTypesFilterGen<M> {
        type Target = ::ethers::contract::Contract<M>;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl<M> ::core::ops::DerefMut for RindexerPlaygroundTypesFilterGen<M> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
    impl<M> ::core::fmt::Debug for RindexerPlaygroundTypesFilterGen<M> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
            f.debug_tuple(::core::stringify!(RindexerPlaygroundTypesFilterGen))
                .field(&self.address())
                .finish()
        }
    }
    impl<M: ::ethers::providers::Middleware> RindexerPlaygroundTypesFilterGen<M> {
        /// Creates a new contract instance with the specified `ethers` client at
        /// `address`. The contract derefs to a `ethers::Contract` object.
        pub fn new<T: Into<::ethers::core::types::Address>>(
            address: T,
            client: ::std::sync::Arc<M>,
        ) -> Self {
            Self(::ethers::contract::Contract::new(
                address.into(),
                RINDEXERPLAYGROUNDTYPESFILTERGEN_ABI.clone(),
                client,
            ))
        }
        ///Gets the contract's `Swap` event
        pub fn swap_filter(
            &self,
        ) -> ::ethers::contract::builders::Event<::std::sync::Arc<M>, M, SwapFilter> {
            self.0.event()
        }
        /// Returns an `Event` builder for all the events of this contract.
        pub fn events(
            &self,
        ) -> ::ethers::contract::builders::Event<::std::sync::Arc<M>, M, SwapFilter> {
            self.0.event_with_filter(::core::default::Default::default())
        }
    }
    impl<M: ::ethers::providers::Middleware> From<::ethers::contract::Contract<M>>
        for RindexerPlaygroundTypesFilterGen<M>
    {
        fn from(contract: ::ethers::contract::Contract<M>) -> Self {
            Self::new(contract.address(), contract.client())
        }
    }
    #[derive(
        Clone,
        ::ethers::contract::EthEvent,
        ::ethers::contract::EthDisplay,
        Default,
        Debug,
        PartialEq,
        Eq,
        Hash,
    )]
    #[ethevent(
        name = "Swap",
        abi = "Swap(address,address,int256,int256,uint160,uint128,int24,int8,int16,int32,int64,int128,int192)"
    )]
    pub struct SwapFilter {
        #[ethevent(indexed)]
        pub sender: ::ethers::core::types::Address,
        #[ethevent(indexed)]
        pub recipient: ::ethers::core::types::Address,
        pub amount_0: ::ethers::core::types::I256,
        pub amount_1: ::ethers::core::types::I256,
        pub sqrt_price_x96: ::ethers::core::types::U256,
        pub liquidity: u128,
        pub tick: i32,
        pub tick_2: i8,
        pub tick_3: i16,
        pub tick_4: i32,
        pub tick_5: i64,
        pub tick_6: i128,
        pub tick_7: ::ethers::core::types::I256,
    }
}
