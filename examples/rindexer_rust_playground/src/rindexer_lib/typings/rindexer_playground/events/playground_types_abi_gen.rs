use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc, all_derives)]
    RindexerPlaygroundTypesGen,
    r#"[
  {
    "anonymous": false,
    "name": "BasicTypes",
    "type": "event",
    "inputs": [
      {
        "name": "aBool",
        "type": "bool",
        "indexed": false
      },
      {
        "name": "simpleAddress",
        "type": "address",
        "indexed": false
      },
      {
        "name": "simpleString",
        "type": "string",
        "indexed": false
      }
    ]
  },
  {
    "anonymous": false,
    "name": "TupleTypes",
    "type": "event",
    "inputs": [
      {
        "name": "array",
        "type": "tuple[]",
        "internalType": "struct SimpleStruct[]",
        "indexed": false,
        "components": [
          {
            "name": "address",
            "type": "address"
          },
          {
            "name": "string",
            "type": "string"
          },
          {
            "name": "fixedBytes",
            "type": "bytes32"
          },
          {
            "name": "dynamicBytes",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "nestedArray",
        "type": "tuple[]",
        "internalType": "struct ComplexStruct[]",
        "indexed": false,
        "components": [
          {
            "name": "ruleParam",
            "type": "tuple",
            "internalType": "struct KeyValueArray",
            "components": [
              {
                "name": "key",
                "type": "bytes32"
              },
              {
                "name": "value",
                "type": "tuple[]",
                "internalType": "struct KeyValue[]",
                "components": [
                  {
                    "name": "key",
                    "type": "bytes32"
                  },
                  {
                    "name": "value",
                    "type": "bytes"
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  },
  {
    "anonymous": false,
    "name": "BytesTypes",
    "type": "event",
    "inputs": [
      {
        "name": "aByte1",
        "type": "bytes1",
        "indexed": false
      },
      {
        "name": "aByte4",
        "type": "bytes4",
        "indexed": false
      },
      {
        "name": "aByte8",
        "type": "bytes8",
        "indexed": false
      },
      {
        "name": "aByte16",
        "type": "bytes16",
        "indexed": false
      },
      {
        "name": "aByte32",
        "type": "bytes32",
        "indexed": false
      },
      {
        "name": "dynamicBytes",
        "type": "bytes",
        "indexed": false
      }
    ]
  },
  {
    "anonymous": false,
    "name": "RegularWidthSignedIntegers",
    "type": "event",
    "inputs": [
      {
        "name": "i8",
        "type": "int8",
        "indexed": false
      },
      {
        "name": "i16",
        "type": "int16",
        "indexed": false
      },
      {
        "name": "i32",
        "type": "int32",
        "indexed": false
      },
      {
        "name": "i64",
        "type": "int64",
        "indexed": false
      },
      {
        "name": "i128",
        "type": "int128",
        "indexed": false
      },
      {
        "name": "i256",
        "type": "int256",
        "indexed": false
      }
    ]
  },
  {
    "anonymous": false,
    "name": "RegularWidthUnsignedIntegers",
    "type": "event",
    "inputs": [
      {
        "name": "u8",
        "type": "uint8",
        "indexed": false
      },
      {
        "name": "u16",
        "type": "uint16",
        "indexed": false
      },
      {
        "name": "u32",
        "type": "uint32",
        "indexed": false
      },
      {
        "name": "u64",
        "type": "uint64",
        "indexed": false
      },
      {
        "name": "u128",
        "type": "uint128",
        "indexed": false
      },
      {
        "name": "u256",
        "type": "uint256",
        "indexed": false
      }
    ]
  },
  {
    "anonymous": false,
    "name": "IrregularWidthSignedIntegers",
    "type": "event",
    "inputs": [
      {
        "name": "i24",
        "type": "int24",
        "indexed": false
      },
      {
        "name": "i40",
        "type": "int40",
        "indexed": false
      },
      {
        "name": "i48",
        "type": "int48",
        "indexed": false
      },
      {
        "name": "i56",
        "type": "int56",
        "indexed": false
      },
      {
        "name": "i72",
        "type": "int72",
        "indexed": false
      },
      {
        "name": "i80",
        "type": "int80",
        "indexed": false
      },
      {
        "name": "i88",
        "type": "int88",
        "indexed": false
      },
      {
        "name": "i96",
        "type": "int96",
        "indexed": false
      },
      {
        "name": "i104",
        "type": "int104",
        "indexed": false
      },
      {
        "name": "i112",
        "type": "int112",
        "indexed": false
      },
      {
        "name": "i120",
        "type": "int120",
        "indexed": false
      },
      {
        "name": "i136",
        "type": "int136",
        "indexed": false
      },
      {
        "name": "i144",
        "type": "int144",
        "indexed": false
      },
      {
        "name": "i152",
        "type": "int152",
        "indexed": false
      },
      {
        "name": "i160",
        "type": "int160",
        "indexed": false
      },
      {
        "name": "i168",
        "type": "int168",
        "indexed": false
      },
      {
        "name": "i176",
        "type": "int176",
        "indexed": false
      },
      {
        "name": "i184",
        "type": "int184",
        "indexed": false
      },
      {
        "name": "i192",
        "type": "int192",
        "indexed": false
      },
      {
        "name": "i200",
        "type": "int200",
        "indexed": false
      },
      {
        "name": "i208",
        "type": "int208",
        "indexed": false
      },
      {
        "name": "i216",
        "type": "int216",
        "indexed": false
      },
      {
        "name": "i224",
        "type": "int224",
        "indexed": false
      },
      {
        "name": "i232",
        "type": "int232",
        "indexed": false
      },
      {
        "name": "i240",
        "type": "int240",
        "indexed": false
      },
      {
        "name": "i248",
        "type": "int248",
        "indexed": false
      }
    ]
  },
  {
    "anonymous": false,
    "name": "IrregularWidthUnsignedIntegers",
    "type": "event",
    "inputs": [
      {
        "name": "u24",
        "type": "uint24",
        "indexed": false
      },
      {
        "name": "u40",
        "type": "uint40",
        "indexed": false
      },
      {
        "name": "u48",
        "type": "uint48",
        "indexed": false
      },
      {
        "name": "u56",
        "type": "uint56",
        "indexed": false
      },
      {
        "name": "u72",
        "type": "uint72",
        "indexed": false
      },
      {
        "name": "u80",
        "type": "uint80",
        "indexed": false
      },
      {
        "name": "u88",
        "type": "uint88",
        "indexed": false
      },
      {
        "name": "u96",
        "type": "uint96",
        "indexed": false
      },
      {
        "name": "u104",
        "type": "uint104",
        "indexed": false
      },
      {
        "name": "u112",
        "type": "uint112",
        "indexed": false
      },
      {
        "name": "u120",
        "type": "uint120",
        "indexed": false
      },
      {
        "name": "u136",
        "type": "uint136",
        "indexed": false
      },
      {
        "name": "u144",
        "type": "uint144",
        "indexed": false
      },
      {
        "name": "u152",
        "type": "uint152",
        "indexed": false
      },
      {
        "name": "u160",
        "type": "uint160",
        "indexed": false
      },
      {
        "name": "u168",
        "type": "uint168",
        "indexed": false
      },
      {
        "name": "u176",
        "type": "uint176",
        "indexed": false
      },
      {
        "name": "u184",
        "type": "uint184",
        "indexed": false
      },
      {
        "name": "u192",
        "type": "uint192",
        "indexed": false
      },
      {
        "name": "u200",
        "type": "uint200",
        "indexed": false
      },
      {
        "name": "u208",
        "type": "uint208",
        "indexed": false
      },
      {
        "name": "u216",
        "type": "uint216",
        "indexed": false
      },
      {
        "name": "u224",
        "type": "uint224",
        "indexed": false
      },
      {
        "name": "u232",
        "type": "uint232",
        "indexed": false
      },
      {
        "name": "u240",
        "type": "uint240",
        "indexed": false
      },
      {
        "name": "u248",
        "type": "uint248",
        "indexed": false
      }
    ]
  },
  {
    "type": "event",
    "name": "Under_Score",
    "inputs": [
      {
        "name": "foo",
        "type": "address",
        "indexed": true,
        "internalType": "address"
      }
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "CAPITALIZED",
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
