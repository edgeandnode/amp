// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {CREATE3} from "solady/utils/CREATE3.sol";

contract Factory {
    function deploy(bytes32 salt, bytes memory creationCode) external payable returns (address deployed) {
        return CREATE3.deployDeterministic(msg.value, creationCode, salt);
    }

    function getDeployed(bytes32 salt) external view returns (address) {
        return CREATE3.predictDeterministicAddress(salt, address(this));
    }
}
