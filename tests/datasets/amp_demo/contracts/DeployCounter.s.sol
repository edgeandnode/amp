// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script, console} from "forge-std/Script.sol";
import {Counter} from "./Counter.sol";

contract DeployCounter is Script {
    function run() public returns (address) {
        vm.startBroadcast();
        Counter counter = new Counter();
        console.log("Counter deployed to:", address(counter));
        vm.stopBroadcast();
        return address(counter);
    }
}
