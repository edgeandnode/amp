// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script as ForgeScript} from "forge-std/Script.sol";
import {Factory} from "./Factory.sol";

abstract contract Script is ForgeScript {
    bytes32 internal constant SALT = 0;
    address internal constant CREATE2 = 0x4e59b44847b379578588920cA78FbF26c0B4956C;

    function getOrDeployFactory() internal returns (Factory) {
        address factoryAddress = vm.computeCreate2Address(
            SALT,
            keccak256(type(Factory).creationCode),
            CREATE2
        );

        if (factoryAddress.code.length == 0) {
            return new Factory{salt: SALT}();
        }

        return Factory(factoryAddress);
    }

    function getFactory() internal pure returns (Factory) {
        address factoryAddress = vm.computeCreate2Address(
            SALT,
            keccak256(type(Factory).creationCode),
            CREATE2
        );

        return Factory(factoryAddress);
    }

    function deploy(bytes32 salt, bytes memory creationCode) internal returns (address) {
        Factory factory = getOrDeployFactory();
        return factory.deploy(salt, creationCode);
    }

    function getDeployed(bytes32 salt) internal view returns (address) {
        Factory factory = getFactory();
        return factory.getDeployed(salt);
    }
}
