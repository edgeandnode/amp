// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

contract Counter {
    uint256 public number;
    event Count(uint256 count);

    function setNumber(uint256 newNumber) public {
        number = newNumber;
        emit Count(number);
    }

    function increment() public {
        number++;
        emit Count(number);
    }
}
