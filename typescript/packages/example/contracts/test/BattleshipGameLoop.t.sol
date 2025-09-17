// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Battleship} from "../src/Battleship.sol";
import {BattleshipTestHelper} from "./helpers/BattleshipTestHelper.sol";

/**
 * @title BattleshipGameLoop Test Suite
 * @dev Core game mechanics testing for Battleship contract
 */
contract BattleshipGameLoopTest is BattleshipTestHelper {
    // ═══════════════════════════════════════════════════════════════════
    // Setup & Initialization Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_CreateGame_Success() public {
        uint256 stake = 1 ether;

        vm.expectEmit(true, true, false, true);
        emit Battleship.GameCreated(0, alice);

        uint256 gameId = createGame(alice, stake);

        assertEq(gameId, 0);
        assertTrue(battleship.isGameValid(gameId));
        assertFalse(battleship.isGameStarted(gameId));

        assertGameState(gameId, alice, address(0), stake, address(0));
    }

    function test_CreateGame_InvalidBoardProof() public {
        setMockVerifierResult(false, true); // Board verifier returns false

        vm.expectRevert(Battleship.InvalidBoardProof.selector);

        vm.prank(alice);
        battleship.createGame{value: 1 ether}(mockBoardProof());
    }

    function test_CreateGame_WithStake() public {
        uint256 stake = 5 ether;
        uint256 gameId = createGame(alice, stake);

        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.prizePool, stake);
    }

    function test_CreateGame_ZeroStake() public {
        uint256 gameId = createGame(alice, 0);

        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.prizePool, 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Player Joining Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_JoinGame_Success() public {
        uint256 stake = 1 ether;
        uint256 gameId = createGame(alice, stake);

        vm.expectEmit(true, true, false, true);
        emit Battleship.PlayerJoined(gameId, bob);

        vm.expectEmit(true, false, false, true);
        emit Battleship.GameStarted(gameId);

        vm.prank(bob);
        battleship.joinGame{value: stake}(gameId, mockBoardProof());

        assertTrue(battleship.isGameStarted(gameId));
        assertGameState(gameId, alice, bob, stake * 2, address(0));
    }

    function test_JoinGame_InvalidBoardProof() public {
        uint256 gameId = createGame(alice, 1 ether);

        setMockVerifierResult(false, true); // Board verifier returns false

        vm.expectRevert(Battleship.InvalidBoardProof.selector);

        vm.prank(bob);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());
    }

    function test_JoinGame_WrongStake() public {
        uint256 gameId = createGame(alice, 1 ether);

        vm.expectRevert(Battleship.InsufficientStake.selector);

        vm.prank(bob);
        battleship.joinGame{value: 0.5 ether}(gameId, mockBoardProof());
    }

    function test_JoinGame_AlreadyFull() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);

        vm.expectRevert(Battleship.GameLobbyFull.selector);

        vm.prank(charlie);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());
    }

    function test_JoinGame_SamePlayer() public {
        uint256 gameId = createGame(alice, 1 ether);

        vm.expectRevert(Battleship.PlayerAlreadyInGame.selector);

        vm.prank(alice);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());
    }

    function test_JoinGame_InvalidGameId() public {
        vm.expectRevert(Battleship.InvalidGameId.selector);

        vm.prank(bob);
        battleship.joinGame{value: 1 ether}(999, mockBoardProof());
    }

    // ═══════════════════════════════════════════════════════════════════
    // Turn Management Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_Attack_FirstMove() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        vm.expectEmit(true, true, false, true);
        emit Battleship.ShotFired(gameId, alice, 5, 5);

        vm.prank(alice);
        battleship.attack(gameId, 5, 5);

        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.lastShotX, 5);
        assertEq(gameState.lastShotY, 5);
        assertEq(gameState.lastPlayer, alice);
    }

    function test_Attack_WrongStartingPlayer() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        // Try with wrong player (bob instead of alice)
        vm.expectRevert(Battleship.NotYourTurn.selector);
        vm.prank(bob);
        battleship.attack(gameId, 3, 3);
    }

    function test_Attack_NonPlayer() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        vm.expectRevert(Battleship.NotAPlayer.selector);
        vm.prank(charlie);
        battleship.attack(gameId, 5, 5);
    }

    function test_Attack_InvalidCoordinates() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        vm.expectRevert(Battleship.InvalidCoordinates.selector);
        vm.prank(alice);
        battleship.attack(gameId, 10, 5); // X coordinate out of bounds
    }

    function test_Attack_OnlyFirstMove() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Try to attack again - should fail since there's a pending response
        vm.expectRevert(Battleship.InvalidMove.selector);

        vm.prank(alice);
        battleship.attack(gameId, 4, 4);
    }

    function test_Attack_GameNotStarted() public {
        uint256 gameId = createGame(alice, 1 ether);

        vm.expectRevert(Battleship.GameNotStarted.selector);

        vm.prank(alice);
        battleship.attack(gameId, 5, 5);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Response & Counter-Attack Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_RespondAndCounter_Success() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address responder = bob;

        vm.expectEmit(true, false, false, true);
        emit Battleship.ImpactReported(gameId, responder, Battleship.Impact.HIT);

        vm.expectEmit(true, true, false, true);
        emit Battleship.ShotFired(gameId, responder, 7, 7);

        respondAndCounter(gameId, responder, Battleship.Impact.HIT, 7, 7);

        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.lastShotX, 7);
        assertEq(gameState.lastShotY, 7);
        assertEq(gameState.lastPlayer, responder);
    }

    function test_RespondAndCounter_WrongCoordinates() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Create proof with wrong coordinates (5, 5 instead of 3, 3)
        Battleship.ImpactProof memory wrongProof = getMockShotProof(5, 5, Battleship.Impact.MISS);

        vm.expectRevert(Battleship.WrongCoordinates.selector);

        vm.prank(bob);
        battleship.respondAndCounter(gameId, wrongProof, 7, 7);
    }

    function test_RespondAndCounter_InvalidProof() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        setMockVerifierResult(true, false); // Shot verifier returns false

        // Get current shot coordinates
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);

        vm.expectRevert(Battleship.InvalidShotProof.selector);

        vm.prank(bob);
        battleship.respondAndCounter(
            gameId, getMockShotProof(gameState.lastShotX, gameState.lastShotY, Battleship.Impact.MISS), 7, 7
        );
    }

    function test_RespondAndCounter_WrongPlayer() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Get current shot coordinates
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);

        vm.expectRevert(Battleship.NotYourTurn.selector);

        // First player tries to respond to their own attack
        vm.prank(alice);
        battleship.respondAndCounter(
            gameId, getMockShotProof(gameState.lastShotX, gameState.lastShotY, Battleship.Impact.MISS), 7, 7
        );
    }

    function test_RespondAndCounter_CommitmentMismatch() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Create proof with wrong commitment
        Battleship.ImpactProof memory wrongCommitmentProof =
            getMockShotProofWithShips(3, 3, Battleship.Impact.MISS, 5, 0x9999);

        vm.expectRevert(Battleship.CommitmentMismatch.selector);

        vm.prank(bob);
        battleship.respondAndCounter(gameId, wrongCommitmentProof, 7, 7);
    }

    function test_RespondAndCounter_InvalidCounterCoordinates() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);

        // Get current shot coordinates
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);

        vm.expectRevert(Battleship.InvalidCoordinates.selector);

        vm.prank(bob);
        battleship.respondAndCounter(
            gameId,
            getMockShotProof(gameState.lastShotX, gameState.lastShotY, Battleship.Impact.MISS),
            10, // X out of bounds
            7
        );
    }

    function test_RespondAndCounter_NoAttackPending() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);

        vm.expectRevert(Battleship.InvalidMove.selector);

        vm.prank(bob);
        battleship.respondAndCounter(gameId, getMockShotProof(0, 0, Battleship.Impact.MISS), 7, 7);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Game State Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_GameFlow_AlternatingTurns() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        // Alice is explicitly the starting player

        // Alice attacks first
        vm.prank(alice);
        battleship.attack(gameId, 1, 1);

        // Bob responds and counters
        respondAndCounter(gameId, bob, Battleship.Impact.MISS, 2, 2);

        // Alice responds and counters
        respondAndCounter(gameId, alice, Battleship.Impact.HIT, 3, 3);

        // Bob responds and counters
        respondAndCounter(gameId, bob, Battleship.Impact.MISS, 4, 4);

        // Verify final state
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.lastShotX, 4);
        assertEq(gameState.lastShotY, 4);
        assertEq(gameState.lastPlayer, bob);
    }

    function test_GameFlow_RandomStartingPlayer() public {
        // Test multiple games to verify randomness (though with fixed block data, it won't be truly random)
        for (uint256 i = 0; i < 5; i++) {
            uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);
            Battleship.Game memory gameState = battleship.getGameInfo(gameId);
            assertEq(gameState.startingPlayer, 0); // Alice is player 0 and starting player

            // Advance block to change entropy for next game
            vm.roll(block.number + 1);
            vm.warp(block.timestamp + 1);
        }
    }

    function test_GameFlow_MultipleRounds() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether, alice);

        vm.prank(alice);
        battleship.attack(gameId, 2, 3);

        respondAndCounter(gameId, bob, Battleship.Impact.MISS, 4, 5);
        respondAndCounter(gameId, alice, Battleship.Impact.HIT, 6, 7);
        respondAndCounter(gameId, bob, Battleship.Impact.MISS, 8, 9);
        respondAndCounter(gameId, alice, Battleship.Impact.SUNK, 1, 0);

        // Verify game is still active
        assertFalse(battleship.isGameEnded(gameId));

        // Verify last shot was recorded
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.lastShotX, 1);
        assertEq(gameState.lastShotY, 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Impact Result Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_Impact_Miss() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address defender = getOtherPlayer(alice);

        vm.expectEmit(true, false, false, true);
        emit Battleship.ImpactReported(gameId, defender, Battleship.Impact.MISS);

        respondAndCounter(gameId, defender, Battleship.Impact.MISS, 7, 7);
    }

    function test_Impact_Hit() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address defender = getOtherPlayer(alice);

        vm.expectEmit(true, false, false, true);
        emit Battleship.ImpactReported(gameId, defender, Battleship.Impact.HIT);

        respondAndCounter(gameId, defender, Battleship.Impact.HIT, 7, 7);
    }

    function test_Impact_Sunk_NotFinalShip() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address defender = getOtherPlayer(alice);

        vm.expectEmit(true, false, false, true);
        emit Battleship.ImpactReported(gameId, defender, Battleship.Impact.SUNK);

        respondAndCounter(gameId, defender, Battleship.Impact.SUNK, 7, 7);

        // Game should continue since isGameOver returns false
        assertFalse(battleship.isGameEnded(gameId));
    }

    function test_Impact_Sunk_FinalShip_GameEnds() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address defender = getOtherPlayer(alice);
        uint256 attackerInitialBalance = alice.balance;

        // Expect GameEnded event when final ship is sunk
        vm.expectEmit(true, true, false, true);
        emit Battleship.GameEnded(gameId, alice);

        // Respond with SUNK and 0 remaining ships (final ship)
        respondAndCounterWithShips(gameId, defender, Battleship.Impact.SUNK, 7, 7, 0);

        // Game should be ended
        assertTrue(battleship.isGameEnded(gameId));

        // Verify winner and payout
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.winner, alice);
        assertEq(gameState.prizePool, 0); // Prize pool should be emptied
        assertEq(alice.balance, attackerInitialBalance + 2 ether); // Winner gets full prize pool
    }

    function test_Impact_Hit_NoGameEnd() public {
        uint256 gameId = setupGameWithFirstAttack(alice, bob, alice, 1 ether, 3, 3);
        address defender = getOtherPlayer(alice);

        // Respond with HIT and 0 remaining ships - game should NOT end (only SUNK triggers game end)
        respondAndCounterWithShips(gameId, defender, Battleship.Impact.HIT, 7, 7, 0);

        // Game should still be active
        assertFalse(battleship.isGameEnded(gameId));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Game Ending Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_ForfeitGame_Success() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);
        uint256 bobInitialBalance = bob.balance;

        vm.expectEmit(true, true, false, true);
        emit Battleship.GameEnded(gameId, bob);

        vm.prank(alice);
        battleship.forfeitGame(gameId);

        // Bob should win and get the prize
        Battleship.Game memory gameState = battleship.getGameInfo(gameId);
        assertEq(gameState.winner, bob);
        assertEq(gameState.prizePool, 0);
        assertEq(bob.balance, bobInitialBalance + 2 ether);
    }

    function test_ForfeitGame_NotPlayer() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);

        vm.expectRevert(Battleship.NotAPlayer.selector);

        vm.prank(charlie);
        battleship.forfeitGame(gameId);
    }

    function test_ForfeitGame_GameNotStarted() public {
        uint256 gameId = createGame(alice, 1 ether);

        vm.expectRevert(Battleship.GameNotStarted.selector);

        vm.prank(alice);
        battleship.forfeitGame(gameId);
    }

    // ═══════════════════════════════════════════════════════════════════
    // View Functions Tests
    // ═══════════════════════════════════════════════════════════════════

    function test_GetGameInfo() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);

        Battleship.Game memory gameState = battleship.getGameInfo(gameId);

        assertEq(gameState.players[0], alice);
        assertEq(gameState.players[1], bob);
        assertEq(uint256(gameState.boardCommitments[0]), MOCK_BOARD_COMMITMENT);
        assertEq(uint256(gameState.boardCommitments[1]), MOCK_BOARD_COMMITMENT);
        assertEq(gameState.prizePool, 2 ether);
        assertEq(gameState.lastPlayer, address(0));
        assertTrue(gameState.startingPlayer == 0 || gameState.startingPlayer == 1);
        assertEq(gameState.winner, address(0));
    }

    function test_IsGameValid() public {
        uint256 gameId = createGame(alice, 1 ether);
        assertTrue(battleship.isGameValid(gameId));
        assertFalse(battleship.isGameValid(999));
    }

    function test_IsGameStarted() public {
        uint256 gameId = createGame(alice, 1 ether);
        assertFalse(battleship.isGameStarted(gameId));

        vm.prank(bob);
        battleship.joinGame{value: 1 ether}(gameId, mockBoardProof());
        assertTrue(battleship.isGameStarted(gameId));
    }

    function test_IsGameEnded() public {
        uint256 gameId = setupTwoPlayerGame(alice, bob, 1 ether);
        assertFalse(battleship.isGameEnded(gameId));

        vm.prank(alice);
        battleship.forfeitGame(gameId);
        assertTrue(battleship.isGameEnded(gameId));
    }
}
