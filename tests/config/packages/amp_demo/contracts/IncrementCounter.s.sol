// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script} from "forge-std/Script.sol";
import {Counter} from "./Counter.sol";

contract IncrementCounter is Script {
    function run(address counterAddr) public {
        vm.startBroadcast();
        Counter(counterAddr).increment();
        vm.stopBroadcast();
    }
}
