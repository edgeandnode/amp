// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Test, console} from "forge-std/Test.sol";
import {Battleship} from "../../src/Battleship.sol";
import {Groth16Verifier as BoardVerifier} from "../../src/BoardVerifier.sol";
import {Groth16Verifier as ImpactVerifier} from "../../src/ImpactVerifier.sol";
import {ProofLibrary} from "../helpers/ProofLibrary.sol";
import {BoardLibrary} from "../helpers/BoardLibrary.sol";

/**
 * @title TestBase
 * @dev Test utilities for Battleship game testing
 */
abstract contract TestBase is Test {
    using ProofLibrary for *;
    using BoardLibrary for *;

    Battleship public bs;

    // Test addresses
    address public alice = makeAddr("alice");
    address public bob = makeAddr("bob");
    address public charlie = makeAddr("charlie");

    // Shared mock commitments - since mock verifiers always return true, we don't need player-specific values
    uint256 public constant MOCK_BOARD_COMMITMENT = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;
    uint256 public constant MOCK_INITIAL_STATE_COMMITMENT =
        0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321;

    function setUpAccounts() public {
        vm.label(alice, "ALICE");
        vm.label(bob, "BOB");
        vm.label(charlie, "CHARLIE");

        vm.deal(alice, 10 ether);
        vm.deal(bob, 10 ether);
        vm.deal(charlie, 10 ether);
    }

    function setUp() public virtual {
        address boardVerifier = address(new BoardVerifier());
        address impactVerifier = address(new ImpactVerifier());
        bs = new Battleship(boardVerifier, impactVerifier);

        vm.label(address(bs), "battleship");
        vm.label(address(boardVerifier), "impact verifier");
        vm.label(address(impactVerifier), "board verifier");

        setUpAccounts();
    }

    function createGame(address player, uint256 stakeAmount, BoardLibrary.BoardConfig memory boardConfig)
        public
        payable
        returns (BoardLibrary.BoardState memory)
    {
        console.log("%s creates game with %s stake", vm.getLabel(player), stakeAmount);

        Battleship.BoardProof memory proof = ProofLibrary.createBoardProof(boardConfig);

        vm.prank(player);
        uint256 gameId = bs.createGame{value: stakeAmount}(proof);

        BoardLibrary.BoardState memory boardState = BoardLibrary.BoardState({
            gameId: gameId,
            player: player,
            ships: [
                boardConfig.carrier,
                boardConfig.battleship,
                boardConfig.cruiser,
                boardConfig.submarine,
                boardConfig.destroyer
            ],
            previousHitCounts: [0, 0, 0, 0, 0],
            boardCommitment: uint256(proof.publicSignals[0]),
            previousCommitment: uint256(proof.publicSignals[1]),
            salt: boardConfig.salt
        });

        console.log("%s successfully created game %s", vm.getLabel(player), gameId);
        return boardState;
    }

    function logBoardState(BoardLibrary.BoardState memory boardState) public view {
        console.log("");
        console.log(
            "========== %s BOARD STATE (GAME ID: %s) ==========",
            vm.toUppercase(vm.getLabel(boardState.player)),
            boardState.gameId
        );
        console.log("SHIPS:");
        for (uint8 i = 0; i < 5; i++) {
            uint8 shipX = boardState.ships[i][0];
            uint8 shipY = boardState.ships[i][1];
            uint8 shipHitpoints = BoardLibrary.shipHitpoints(i) - boardState.previousHitCounts[i];
            string memory shipOrientation = BoardLibrary.orientationName(boardState.ships[i][2]);
            string memory shipPlacement =
                string.concat(shipOrientation, " at (", vm.toString(shipX), ",", vm.toString(shipY), ")");
            string memory shipName = vm.toUppercase(BoardLibrary.shipName(i));

            console.log("  %s: %s with %s hitpoints", shipName, shipPlacement, shipHitpoints);
        }

        console.log("HIT COUNTS:");
        for (uint8 i = 0; i < 5; i++) {
            string memory shipName = vm.toUppercase(BoardLibrary.shipName(i));
            console.log("  %s: %s", shipName, boardState.previousHitCounts[i]);
        }

        console.log("STATE COMMITMENT:", boardState.previousCommitment);
        console.log("BOARD COMMITMENT:", boardState.boardCommitment);
        console.log("SALT:", boardState.salt);
        console.log("");
    }

    function joinGame(address player, uint256 gameId, uint256 stakeAmount, BoardLibrary.BoardConfig memory boardConfig)
        public
        payable
        returns (BoardLibrary.BoardState memory boardState)
    {
        console.log("%s joins game %s with %s stake", vm.getLabel(player), gameId, stakeAmount);

        Battleship.BoardProof memory proof = ProofLibrary.createBoardProof(boardConfig);

        vm.prank(player);
        bs.joinGame{value: stakeAmount}(gameId, proof);

        boardState = BoardLibrary.BoardState({
            gameId: gameId,
            player: player,
            ships: [
                boardConfig.carrier,
                boardConfig.battleship,
                boardConfig.cruiser,
                boardConfig.submarine,
                boardConfig.destroyer
            ],
            previousHitCounts: [0, 0, 0, 0, 0],
            boardCommitment: uint256(proof.publicSignals[0]),
            previousCommitment: uint256(proof.publicSignals[1]),
            salt: boardConfig.salt
        });

        console.log("%s successfully joined game %s", vm.getLabel(player), gameId);
    }

    function initialAttack(BoardLibrary.BoardState memory boardState, uint8 targetX, uint8 targetY) public {
        console.log("%s attacks coordinates (%s,%s)", vm.getLabel(boardState.player), targetX, targetY);

        vm.prank(boardState.player);
        bs.attack(boardState.gameId, targetX, targetY);
    }

    function counterAttack(
        BoardLibrary.BoardState memory boardState,
        Battleship.Impact claimedResult,
        uint8 hitShipId,
        uint8 counterX,
        uint8 counterY
    ) public returns (BoardLibrary.BoardState memory) {
        if (claimedResult == Battleship.Impact.MISS) {
            require(hitShipId == 255, "Invalid ship identifier for miss");
        } else {
            require(hitShipId <= 4, "Invalid ship identifier for hit or sunk");
        }

        if (claimedResult == Battleship.Impact.MISS) {
            console.log("%s responds MISS", vm.getLabel(boardState.player));
        } else {
            string memory resultString = vm.toUppercase(BoardLibrary.impactName(claimedResult));
            string memory shipString = vm.toUppercase(BoardLibrary.shipName(hitShipId));
            console.log("%s responds %s on %s", vm.getLabel(boardState.player), resultString, shipString);
        }

        console.log("%s attacks coordinates (%s,%s)", vm.getLabel(boardState.player), counterX, counterY);

        Battleship.Game memory gameState = bs.getGameInfo(boardState.gameId);
        Battleship.ImpactProof memory proof = ProofLibrary.createImpactProof({
            previousCommitment: boardState.previousCommitment,
            targetX: gameState.lastShotX,
            targetY: gameState.lastShotY,
            claimedResult: uint8(claimedResult),
            claimedShipId: claimedResult == Battleship.Impact.SUNK ? hitShipId : 255,
            boardCommitment: boardState.boardCommitment,
            ships: boardState.ships,
            previousHitCounts: boardState.previousHitCounts,
            salt: boardState.salt
        });

        vm.prank(boardState.player);
        bs.respondAndCounter(boardState.gameId, proof, counterX, counterY);

        BoardLibrary.BoardState memory newBoardState = BoardLibrary.BoardState({
            gameId: boardState.gameId,
            player: boardState.player,
            ships: boardState.ships,
            previousHitCounts: boardState.previousHitCounts,
            previousCommitment: proof.publicSignals[0],
            boardCommitment: boardState.boardCommitment,
            salt: boardState.salt
        });

        // Update the previous hit counts if necessary
        if (claimedResult == Battleship.Impact.HIT || claimedResult == Battleship.Impact.SUNK) {
            newBoardState.previousHitCounts[hitShipId]++;
        }

        return newBoardState;
    }

    /**
     * @dev Get shared mock board proof - since mock verifiers always return true, we can reuse the same proof
     */
    function mockBoardProof() public pure returns (Battleship.BoardProof memory) {
        return Battleship.BoardProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicSignals: [MOCK_BOARD_COMMITMENT, MOCK_INITIAL_STATE_COMMITMENT]
        });
    }

    /**
     * @dev Generate a mock shot proof
     */
    function getMockShotProof(uint256 gameId, address player, Battleship.Impact result)
        public
        view
        returns (Battleship.ImpactProof memory)
    {
        // Default to 5 remaining ships (not game over) and standard commitment
        return getMockShotProofWithShips(gameId, player, result, 5);
    }

    /**
     * @dev Generate a mock shot proof with specific remaining ships count and commitment
     */
    function getMockShotProofWithShips(uint256 gameId, address player, Battleship.Impact result, uint256 remainingShips)
        public
        view
        returns (Battleship.ImpactProof memory)
    {
        Battleship.Game memory gameState = bs.getGameInfo(gameId);
        bytes32 previousCommitment = bs.getStateCommitment(gameId, player);
        bytes32 boardCommitment = bs.getBoardCommitment(gameId, player);
        return Battleship.ImpactProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicSignals: [
                uint256(previousCommitment), // newCommitment (index 0) - same for miss
                remainingShips, // remainingShips (index 1)
                uint256(previousCommitment), // previousCommitment (index 2)
                uint256(gameState.lastShotX), // targetX (index 3)
                uint256(gameState.lastShotY), // targetY (index 4)
                uint256(result), // claimedResult (index 5)
                uint256(255), // claimedShipId (index 6) - 255 for not sunk
                uint256(boardCommitment) // boardCommitment (index 7)
            ]
        });
    }

    /**
     * @dev Assert game state matches expected values
     */
    function assertGameState(
        uint256 gameId,
        address expectedPlayer0,
        address expectedPlayer1,
        uint256 expectedPrizePool,
        address expectedWinner
    ) public view {
        Battleship.Game memory gameState = bs.getGameInfo(gameId);

        assertEq(gameState.players[0], expectedPlayer0, "Player 0 mismatch");
        assertEq(gameState.players[1], expectedPlayer1, "Player 1 mismatch");
        assertEq(gameState.prizePool, expectedPrizePool, "Prize pool mismatch");
        assertEq(gameState.winner, expectedWinner, "Winner mismatch");
    }

    /**
     * @dev Assert that game ended with expected winner
     */
    function assertGameWon(uint256 gameId, address expectedWinner) public view {
        Battleship.Game memory gameState = bs.getGameInfo(gameId);
        assertEq(gameState.winner, expectedWinner, "Game winner mismatch");
        assertTrue(gameState.winner != address(0), "Game should have ended");
    }

    /**
     * @dev Assert game is still in progress
     */
    function assertGameInProgress(uint256 gameId) public view {
        Battleship.Game memory gameState = bs.getGameInfo(gameId);
        assertEq(gameState.winner, address(0), "Game should still be in progress");
    }

    /**
     * @dev Assert that last shot coordinates match expected
     */
    function assertLastShot(uint256 gameId, uint8 expectedX, uint8 expectedY) public view {
        Battleship.Game memory gameState = bs.getGameInfo(gameId);
        assertEq(gameState.lastShotX, expectedX, "Last shot X mismatch");
        assertEq(gameState.lastShotY, expectedY, "Last shot Y mismatch");
    }

    /**
     * @dev Set the starting player for a game by overriding storage
     * @param gameId The game ID
     * @param startingPlayer The address that should start (must be player 0 or 1)
     */
    function setStartingPlayer(uint256 gameId, address startingPlayer) public {
        // Get game players to determine the correct index
        Battleship.Game memory gameState = bs.getGameInfo(gameId);

        uint8 playerIndex;
        if (gameState.players[0] == startingPlayer) {
            playerIndex = 0;
        } else if (gameState.players[1] == startingPlayer) {
            playerIndex = 1;
        } else {
            revert("Player not in game");
        }

        // Calculate storage slot for games[gameId].startingPlayer
        // Game struct layout in storage (updated):
        // Slot 0-1: address[2] players
        // Slot 2-3: bytes32[2] boardCommitments
        // Slot 4-5: bytes32[2] stateCommitments
        // Slot 6-7: uint256[2] shotGrids
        // Slot 8: uint256 stakeAmount
        // Slot 9: uint256 prizePool
        // Slot 10: uint8 lastShotX + uint8 lastShotY + address lastPlayer + uint8 startingPlayer (packed)
        // Slot 11: address winner
        bytes32 gameSlot = keccak256(abi.encode(gameId, uint256(1)));
        bytes32 startingPlayerSlot = bytes32(uint256(gameSlot) + 10);

        // startingPlayer is packed with other fields in slot 10
        // Read current value to preserve other fields (lastShotX, lastShotY, lastPlayer)
        bytes32 currentValue = vm.load(address(bs), startingPlayerSlot);

        // startingPlayer is at byte 22 (after 1+1+20 bytes for the other fields)
        // Clear byte 22 and set the new startingPlayer value
        bytes32 mask = ~(bytes32(uint256(0xff)) << (22 * 8)); // Clear byte 22
        bytes32 newValue = (currentValue & mask) | (bytes32(uint256(playerIndex)) << (22 * 8));
        vm.store(address(bs), startingPlayerSlot, newValue);
    }
}
