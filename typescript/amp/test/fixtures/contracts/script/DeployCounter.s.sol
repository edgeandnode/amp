// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script} from "./utils/Script.sol";
import {Counter} from "../src/Counter.sol";

contract DeployCounterScript is Script {
    function run() public {
        vm.startBroadcast();
        deploy(keccak256("counter"), type(Counter).creationCode);
        vm.stopBroadcast();
    }
}
