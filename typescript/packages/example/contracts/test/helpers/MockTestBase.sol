// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {TestBase} from "./TestBase.sol";
import {Battleship} from "../../src/Battleship.sol";
import {MockBoardVerifier, MockImpactVerifier} from "./MockVerifiers.sol";

/**
 * @title MockTestBase
 * @dev Test utilities for Battleship game testing
 */
abstract contract MockTestBase is TestBase {
    MockBoardVerifier public boardVerifier;
    MockImpactVerifier public impactVerifier;

    function setUp() public override {
        boardVerifier = new MockBoardVerifier();
        impactVerifier = new MockImpactVerifier();
        bs = new Battleship(address(boardVerifier), address(impactVerifier));

        vm.label(address(bs), "battleship");
        vm.label(address(boardVerifier), "impact verifier");
        vm.label(address(impactVerifier), "board verifier");

        setUpAccounts();
    }

    function setMockVerifierResult(bool boardVerifierResult, bool impactVerifierResult) public {
        boardVerifier.setShouldReturnTrue(boardVerifierResult);
        impactVerifier.setShouldReturnTrue(impactVerifierResult);
    }
}
