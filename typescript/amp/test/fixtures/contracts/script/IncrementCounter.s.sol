// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script} from "./utils/Script.sol";
import {Counter} from "../src/Counter.sol";

contract IncrementCounterScript is Script {
    function run() public {
        vm.startBroadcast();
        address counter = getDeployed(keccak256("counter"));
        Counter(counter).increment();
        vm.stopBroadcast();
    }
}
