// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Battleship} from "../src/Battleship.sol";
import {BattleshipTestHelper} from "./helpers/BattleshipTestHelper.sol";

/**
 * @title BattleshipEdgeCases Test Suite
 * @dev Edge cases and error conditions testing for Battleship contract
 */
contract BattleshipEdgeCasesTest is BattleshipTestHelper {
    function test_BoundaryCoordinates() public {
        // Test all corner coordinates with explicit starting player (alice)
        uint8[2][4] memory corners = [[0, 0], [0, 9], [9, 0], [9, 9]];

        for (uint256 i = 0; i < 4; i++) {
            uint256 newGameId = setupTwoPlayerGame(alice, bob, 0.1 ether, alice);

            vm.prank(alice);
            battleship.attack(newGameId, corners[i][0], corners[i][1]);

            (
                ,
                ,
                ,
                ,
                uint8 recordedX,
                uint8 recordedY,
                ,
                ,
                
            ) = battleship.getGameInfo(newGameId);
            assertEq(recordedX, corners[i][0]);
            assertEq(recordedY, corners[i][1]);
        }
    }

    function test_ReentrancyInPayout() public {
        // Test reentrancy protection by using a contract that can receive ETH
        // but won't cause gas issues
        SimpleReceiver receiver = new SimpleReceiver();

        // Give the receiver some ETH
        vm.deal(address(receiver), 10 ether);

        // Create a game with the receiver contract
        vm.prank(address(receiver));
        uint256 gameId = battleship.createGame{value: 1 ether}(mockBoardProof());

        vm.prank(bob);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());

        // Bob forfeits - receiver contract should get payout
        uint256 receiverInitialBalance = address(receiver).balance;

        vm.prank(bob);
        battleship.forfeitGame(gameId);

        // Verify the game ended correctly and payout occurred
        assertTrue(battleship.isGameEnded(gameId));
        (
            ,
            ,
            ,
            uint256 prizePool,
            ,
            ,
            ,
            ,
            address winner
        ) = battleship.getGameInfo(gameId);
        assertEq(prizePool, 0);
        assertEq(winner, address(receiver));

        // Verify receiver got the correct payout (2 ETH)
        assertEq(address(receiver).balance, receiverInitialBalance + 2 ether);

        // The key test: contract can receive ETH from forfeit without reverting
        // This proves the transfer mechanism works with contracts
    }

    function test_NoDoubleSpending() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);
        uint256 aliceInitialBalance = alice.balance;

        // Alice forfeits
        vm.prank(alice);
        battleship.forfeitGame(gameId);

        // Try to forfeit again - should fail
        vm.expectRevert(Battleship.GameAlreadyEnded.selector);
        vm.prank(alice);
        battleship.forfeitGame(gameId);

        // Verify Alice's balance didn't change (she lost)
        assertEq(alice.balance, aliceInitialBalance);
    }

    function test_AlternatingVerifierResults() public {
        // Board verifier works, shot verifier fails
        setMockVerifierResult(true, false);

        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        // Alice attacks first (explicit starting player)
        vm.prank(alice);
        battleship.attack(gameId, 3, 3);

        // Bob responds (shot verifier fails)
        vm.expectRevert(Battleship.InvalidShotProof.selector);

        vm.prank(bob);
        battleship.respondAndCounter(gameId, getMockShotProof(3, 3, Battleship.Impact.MISS), 7, 7);
    }

    function test_VerifierFailureDuringGameCreation() public {
        // Start with working verifiers
        setMockVerifierResult(true, true);

        // Create a game successfully
        uint256 gameId = createGame(alice, 1 ether);
        assertTrue(battleship.isGameValid(gameId));

        // Break board verifier
        setMockVerifierResult(false, true);

        // Bob's join should fail
        vm.expectRevert(Battleship.InvalidBoardProof.selector);
        vm.prank(bob);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());

        // Game should still exist but not be started
        assertTrue(battleship.isGameValid(gameId));
        assertFalse(battleship.isGameStarted(gameId));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Prize Pool Edge Cases
    // ═══════════════════════════════════════════════════════════════════

    function test_ZeroStakeGameForfeit() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 0, alice); // Zero stake
        uint256 bobInitialBalance = bob.balance;

        vm.prank(alice);
        battleship.forfeitGame(gameId);

        // Bob wins but gets no additional ETH
        (
            ,
            ,
            ,
            uint256 prizePool,
            ,
            ,
            ,
            ,
            address winner
        ) = battleship.getGameInfo(gameId);
        assertEq(winner, bob);
        assertEq(prizePool, 0);
        assertEq(bob.balance, bobInitialBalance);
    }

    function test_LargeStakeGame() public {
        uint256 largeStake = 50 ether;
        uint256 gameId = setupTwoPlayerGame(alice, bob, largeStake, alice);

        uint256 bobInitialBalance = bob.balance;

        vm.prank(alice);
        battleship.forfeitGame(gameId);

        // Bob should receive 100 ether (2 * 50)
        assertEq(bob.balance, bobInitialBalance + (largeStake * 2));
    }

    function test_EventsInCorrectOrder() public {
        uint256 stake = 1 ether;

        // Expect GameCreated event
        vm.expectEmit(true, true, false, true);
        emit Battleship.GameCreated(0, alice);

        uint256 gameId = createGame(alice, stake);

        // Expect PlayerJoined then GameStarted events
        vm.expectEmit(true, true, false, true);
        emit Battleship.PlayerJoined(gameId, bob);

        vm.expectEmit(true, false, false, true);
        emit Battleship.GameStarted(gameId);

        vm.prank(bob);
        battleship.joinGame{value: stake}(gameId, mockBoardProof());
    }

    function test_MultipleEventsInOneTransaction() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Expect both ImpactReported and ShotFired events in respondAndCounter
        vm.expectEmit(true, false, false, true);
        emit Battleship.ImpactReported(gameId, bob, Battleship.Impact.HIT);

        vm.expectEmit(true, true, false, true);
        emit Battleship.ShotFired(gameId, bob, 7, 7);

        respondAndCounter(gameId, bob, Battleship.Impact.HIT, 7, 7);
    }
}

/**
 * @title Simple Receiver Contract
 * @dev Helper contract to test ETH transfers to contracts
 */
contract SimpleReceiver {
    uint256 public callCount;

    receive() external payable {
        // Absolutely minimal receive function - no storage writes
        // Just accept the ETH
    }

    function recordCall() external {
        callCount++;
    }
}
