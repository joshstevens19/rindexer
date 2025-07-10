use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc, all_derives)]
    RindexerPlaygroundTypesFilterGen,
    r#"[
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "sender",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "recipient",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "int256",
        "name": "amount0",
        "type": "int256"
      },
      {
        "indexed": false,
        "internalType": "int256",
        "name": "amount1",
        "type": "int256"
      },
      {
        "indexed": false,
        "internalType": "uint160",
        "name": "sqrtPriceX96",
        "type": "uint160"
      },
      {
        "indexed": false,
        "internalType": "uint128",
        "name": "liquidity",
        "type": "uint128"
      },
      {
        "indexed": false,
        "internalType": "int24",
        "name": "tick",
        "type": "int24"
      },
      {
        "indexed": false,
        "internalType": "int8",
        "name": "tick2",
        "type": "int8"
      },
      {
        "indexed": false,
        "internalType": "int16",
        "name": "tick3",
        "type": "int16"
      },
      {
        "indexed": false,
        "internalType": "int32",
        "name": "tick4",
        "type": "int32"
      },
      {
        "indexed": false,
        "internalType": "int64",
        "name": "tick5",
        "type": "int64"
      },
      {
        "indexed": false,
        "internalType": "int128",
        "name": "tick6",
        "type": "int128"
      },
      {
        "indexed": false,
        "internalType": "int192",
        "name": "tick7",
        "type": "int192"
      }
    ],
    "name": "Swap",
    "type": "event"
  },
  {
    "type": "event",
    "name": "Two_Word",
    "inputs": [
      {
        "name": "foo",
        "type": "address",
        "indexed": true,
        "internalType": "address"
      }
    ],
    "anonymous": false
  }
]
"#
);
