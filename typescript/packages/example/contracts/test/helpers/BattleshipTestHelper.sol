// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {Battleship} from "../../src/Battleship.sol";
import {MockBoardVerifier, MockShotVerifier} from "../mocks/MockVerifiers.sol";

/**
 * @title BattleshipTestHelper
 * @dev Test utilities for Battleship game testing
 */
contract BattleshipTestHelper is Test {
    Battleship public battleship;
    MockBoardVerifier public mockBoardVerifier;
    MockShotVerifier public mockShotVerifier;

    // Test addresses
    address public alice = makeAddr("alice");
    address public bob = makeAddr("bob");
    address public charlie = makeAddr("charlie");

    // Shared mock board commitment - since mock verifiers always return true, we don't need player-specific values
    uint256 public constant MOCK_BOARD_COMMITMENT = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;

    /**
     * @dev Get shared mock board proof - since mock verifiers always return true, we can reuse the same proof
     */
    function mockBoardProof() public pure returns (Battleship.BoardProof memory) {
        return Battleship.BoardProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicInputs: [MOCK_BOARD_COMMITMENT]
        });
    }

    function setUp() public virtual {
        // Deploy mock verifiers
        mockBoardVerifier = new MockBoardVerifier();
        mockShotVerifier = new MockShotVerifier();

        // Deploy battleship contract
        battleship = new Battleship(address(mockBoardVerifier), address(mockShotVerifier));

        // Give test addresses some ETH
        vm.deal(alice, 100 ether);
        vm.deal(bob, 100 ether);
        vm.deal(charlie, 100 ether);
    }

    /**
     * @dev Generate a mock shot proof
     */
    function getMockShotProof(uint8 x, uint8 y, Battleship.Impact result)
        public
        pure
        returns (Battleship.ImpactProof memory)
    {
        // Default to 5 remaining ships (not game over) and standard commitment
        return getMockShotProofWithShips(x, y, result, 5, MOCK_BOARD_COMMITMENT);
    }

    /**
     * @dev Generate a mock shot proof with specific remaining ships count and commitment
     */
    function getMockShotProofWithShips(
        uint8 x,
        uint8 y,
        Battleship.Impact result,
        uint256 remainingShips,
        uint256 boardCommitment
    ) public pure returns (Battleship.ImpactProof memory) {
        return Battleship.ImpactProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicInputs: [
                boardCommitment, // previousCommitment (index 0)
                boardCommitment, // newCommitment (index 1) - same for miss
                uint256(x), // targetX (index 2)
                uint256(y), // targetY (index 3)
                uint256(result), // claimedResult (index 4)
                uint256(255), // claimedShipId (index 5) - 255 for not sunk
                remainingShips // claimedRemainingShips (index 6)
            ]
        });
    }

    /**
     * @dev Setup game and make first attack with explicit starting player
     */
    function setupGameWithFirstAttack(
        address player1,
        address player2,
        address firstPlayer,
        uint256 stake,
        uint8 targetX,
        uint8 targetY
    ) public returns (uint256 gameId) {
        gameId = setupTwoPlayerGame(player1, player2, stake, firstPlayer);

        // Make first attack
        vm.prank(firstPlayer);
        battleship.attack(gameId, targetX, targetY);
    }

    /**
     * @dev Execute a respond and counter cycle
     */
    function respondAndCounter(
        uint256 gameId,
        address responder,
        Battleship.Impact impact,
        uint8 counterX,
        uint8 counterY
    ) public {
        // Default to 5 remaining ships (game continues)
        respondAndCounterWithShips(gameId, responder, impact, counterX, counterY, 5);
    }

    /**
     * @dev Execute a respond and counter cycle with specific remaining ships count
     */
    function respondAndCounterWithShips(
        uint256 gameId,
        address responder,
        Battleship.Impact impact,
        uint8 counterX,
        uint8 counterY,
        uint256 remainingShips
    ) public {
        // Get current shot coordinates
        (
            ,
            ,
            ,
            ,
            uint8 lastShotX,
            uint8 lastShotY,
            ,
            ,
            
        ) = battleship.getGameInfo(gameId);

        vm.prank(responder);
        battleship.respondAndCounter(
            gameId,
            getMockShotProofWithShips(lastShotX, lastShotY, impact, remainingShips, MOCK_BOARD_COMMITMENT),
            counterX,
            counterY
        );
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
        (
            address[2] memory players,
            ,
            ,
            uint256 prizePool,
            ,
            ,
            ,
            ,
            address winner
        ) = battleship.getGameInfo(gameId);

        assertEq(players[0], expectedPlayer0, "Player 0 mismatch");
        assertEq(players[1], expectedPlayer1, "Player 1 mismatch");
        assertEq(prizePool, expectedPrizePool, "Prize pool mismatch");
        assertEq(winner, expectedWinner, "Winner mismatch");
    }

    /**
     * @dev Enable/disable mock verifier responses
     */
    function setMockVerifierResult(bool boardResult, bool shotResult) public {
        mockBoardVerifier.setShouldReturnTrue(boardResult);
        mockShotVerifier.setShouldReturnTrue(shotResult);
    }

    /**
     * @dev Get the starting and non-starting players for a game
     */
    function getGamePlayers(uint256 gameId) public view returns (address startingPlayer, address otherPlayer) {
        (
            address[2] memory players,
            ,
            ,
            ,
            ,
            ,
            ,
            uint8 startingPlayerIndex,
            
        ) = battleship.getGameInfo(gameId);
        startingPlayer = players[startingPlayerIndex];
        otherPlayer = players[1 - startingPlayerIndex];
    }

    /**
     * @dev Set the starting player for a game by overriding storage
     * @param gameId The game ID
     * @param startingPlayer The address that should start (must be player 0 or 1)
     */
    function setStartingPlayer(uint256 gameId, address startingPlayer) public {
        // Get game players to determine the correct index
        (
            address[2] memory players,
            ,
            ,
            ,
            ,
            ,
            ,
            ,
            
        ) = battleship.getGameInfo(gameId);

        uint8 playerIndex;
        if (players[0] == startingPlayer) {
            playerIndex = 0;
        } else if (players[1] == startingPlayer) {
            playerIndex = 1;
        } else {
            revert("Player not in game");
        }

        // Calculate storage slot for games[gameId].startingPlayer
        // Game struct layout in storage (updated):
        // Slot 0-1: address[2] players
        // Slot 2-3: bytes32[2] boardCommitments
        // Slot 4-5: uint256[2] shotGrids
        // Slot 6: uint256 stakeAmount
        // Slot 7: uint256 prizePool
        // Slot 8: uint8 lastShotX + uint8 lastShotY + address lastPlayer + uint8 startingPlayer (packed)
        // Slot 9: address winner
        bytes32 gameSlot = keccak256(abi.encode(gameId, uint256(1)));
        bytes32 startingPlayerSlot = bytes32(uint256(gameSlot) + 8);

        // startingPlayer is packed with other fields in slot 8
        // Read current value to preserve other fields (lastShotX, lastShotY, lastPlayer)
        bytes32 currentValue = vm.load(address(battleship), startingPlayerSlot);

        // startingPlayer is at byte 22 (after 1+1+20 bytes for the other fields)
        // Clear byte 22 and set the new startingPlayer value
        bytes32 mask = ~(bytes32(uint256(0xff)) << (22 * 8)); // Clear byte 22
        bytes32 newValue = (currentValue & mask) | (bytes32(uint256(playerIndex)) << (22 * 8));
        vm.store(address(battleship), startingPlayerSlot, newValue);
    }

    /**
     * @dev Create a game with explicit creator
     */
    function createGame(address creator, uint256 stake) public returns (uint256 gameId) {
        vm.prank(creator);
        gameId = battleship.createGame{value: stake}(mockBoardProof());
    }

    /**
     * @dev Join a game with explicit joiner
     */
    function joinGame(uint256 gameId, address joiner, uint256 stake) public {
        vm.prank(joiner);
        battleship.joinGame{value: stake}(gameId, mockBoardProof());
    }

    /**
     * @dev Setup a complete two-player game with explicit addresses
     */
    function setupTwoPlayerGame(address player1, address player2, uint256 stake) public returns (uint256 gameId) {
        gameId = createGame(player1, stake);
        joinGame(gameId, player2, stake);
    }

    /**
     * @dev Setup a complete two-player game with explicit starting player
     */
    function setupTwoPlayerGame(address player1, address player2, uint256 stake, address startingPlayer)
        public
        returns (uint256 gameId)
    {
        gameId = setupTwoPlayerGame(player1, player2, stake);
        setStartingPlayer(gameId, startingPlayer);
    }

    /**
     * @dev Get the other player (legacy helper - kept for compatibility)
     */
    function getOtherPlayer(address player) public view returns (address) {
        if (player == alice) return bob;
        if (player == bob) return alice;
        return charlie;
    }
}
