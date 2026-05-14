// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

// Test-only contract used by the e2e harness to verify that indexed string
// event arguments are indexed as keccak256 topic hashes and stored as hex
// strings, while the matching unindexed string remains plaintext.
contract MessageEmitter {
    event MessageEmitted(string indexed indexedMessage, string unindexedMessage);

    function emitMessage() public {
        emit MessageEmitted("a message", "a message");
    }
}
