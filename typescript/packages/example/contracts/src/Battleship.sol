// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

interface IBoardVerifier {
    function verifyProof(uint256[2] memory a, uint256[2][2] memory b, uint256[2] memory c, uint256[2] memory input)
        external
        view
        returns (bool);
}

interface IShotVerifier {
    function verifyProof(uint256[2] memory a, uint256[2][2] memory b, uint256[2] memory c, uint256[8] memory input)
        external
        view
        returns (bool);
}

/**
 * @title Battleship
 * @dev Every response requires a zkSNARK proof. No trust, only verify.
 */
contract Battleship {
    enum Impact {
        MISS,
        HIT,
        SUNK
    }

    struct ImpactProof {
        uint256[2] piA; // π_A: G1 point (x, y)
        uint256[2][2] piB; // π_B: G2 point [[x1, x2], [y1, y2]]
        uint256[2] piC; // π_C: G1 point (x, y)
        uint256[8] publicInputs; // 8 public inputs
    }

    struct BoardProof {
        uint256[2] piA; // π_A: G1 point (x, y)
        uint256[2][2] piB; // π_B: G2 point [[x1, x2], [y1, y2]]
        uint256[2] piC; // π_C: G1 point (x, y)
        uint256[2] publicInputs; // [boardCommitment, initialStateCommitment]
    }

    struct Game {
        address[2] players; // Exactly 2 players
        bytes32[2] boardCommitments; // Board commitments for players[0] and players[1]
        bytes32[2] stateCommitments; // Current state commitments for players[0] and players[1]
        uint256[2] shotGrids; // Per-player shot tracking (100-bit grid per player)
        uint256 stakeAmount; // Individual stake amount required
        uint256 prizePool; // Total ETH in the game (stakes)
        uint8 lastShotX; // Last shot X coordinate (0-9 valid)
        uint8 lastShotY; // Last shot Y coordinate (0-9 valid)
        address lastPlayer; // address(0) = no moves yet, otherwise last player who moved
        uint8 startingPlayer; // 0 or 1, randomly chosen at game start
        address winner;
    }

    IBoardVerifier public immutable BOARD_VERIFIER;
    IShotVerifier public immutable SHOT_VERIFIER;

    uint256 public nextGameId;
    mapping(uint256 => Game) public games;

    // Events - Complete game history reconstructable from these
    event GameCreated(uint256 indexed gameId, address indexed creator);

    event PlayerJoined(uint256 indexed gameId, address indexed player);

    event GameStarted(uint256 indexed gameId);

    event ShotFired(uint256 indexed gameId, address indexed attacker, uint8 targetX, uint8 targetY);

    event ImpactReported(uint256 indexed gameId, address defender, Impact result);

    event GameEnded(uint256 indexed gameId, address indexed winner);

    // Errors
    error InvalidGameId();
    error NotYourTurn();
    error GameAlreadyEnded();
    error GameNotStarted();
    error InvalidPlayer();
    error InsufficientStake();
    error NotAPlayer();
    error InvalidBoardProof();
    error InvalidShotProof();
    error CommitmentMismatch();
    error InvalidCoordinates();
    error InvalidMove();
    error WrongCoordinates();
    error WinnerMustBePlayer();
    error GameLobbyFull();
    error PlayerAlreadyInGame();
    error GameAlreadyStarted();
    error AlreadyShot();

    constructor(address _boardVerifier, address _shotVerifier) {
        BOARD_VERIFIER = IBoardVerifier(_boardVerifier);
        SHOT_VERIFIER = IShotVerifier(_shotVerifier);
    }

    /**
     * @dev Create a new 2-player game with board commitment
     */
    function createGame(BoardProof memory proof) external payable returns (uint256 gameId) {
        if (!BOARD_VERIFIER.verifyProof(proof.piA, proof.piB, proof.piC, proof.publicInputs)) {
            revert InvalidBoardProof();
        }

        gameId = nextGameId++;
        Game storage game = games[gameId];

        game.players[0] = msg.sender;
        game.stakeAmount = msg.value;
        game.prizePool = msg.value; // Initialize with creator's stake

        // Store board and initial state commitments
        game.boardCommitments[0] = bytes32(proof.publicInputs[0]);
        game.stateCommitments[0] = bytes32(proof.publicInputs[1]);

        emit GameCreated(gameId, msg.sender);
    }

    /**
     * @dev Join an existing 2-player game with board commitment
     */
    function joinGame(uint256 gameId, BoardProof memory proof)
        external
        payable
        validGame(gameId)
        gameNotFull(gameId)
        correctStake(gameId)
        notSamePlayer(gameId)
    {
        Game storage game = games[gameId];

        // Verify board is valid
        if (!BOARD_VERIFIER.verifyProof(proof.piA, proof.piB, proof.piC, proof.publicInputs)) {
            revert InvalidBoardProof();
        }

        game.players[1] = msg.sender;
        game.prizePool += msg.value; // Add joining player's stake to prize pool

        // Store board and initial state commitments
        game.boardCommitments[1] = bytes32(proof.publicInputs[0]);
        game.stateCommitments[1] = bytes32(proof.publicInputs[1]);

        // Set random starting player (0 or 1)
        game.startingPlayer = uint8(randomPlayer(gameId));

        // No moves made yet (address(0) = first move)
        game.lastPlayer = address(0);

        emit PlayerJoined(gameId, msg.sender);
        emit GameStarted(gameId);
    }

    /**
     * @dev Forfeit/abandon game after it has started - opponent wins
     */
    function forfeitGame(uint256 gameId) external gameActive(gameId) onlyPlayer(gameId) {
        Game storage game = games[gameId];
        // Determine winner (the other player)
        game.winner = getOtherPlayer(gameId, msg.sender);
        // Winner gets entire prize pool
        uint256 totalPayout = game.prizePool;
        game.prizePool = 0;
        payable(game.winner).transfer(totalPayout);
        emit GameEnded(gameId, game.winner);
    }

    /**
     * @dev Cancel a game before it starts and refund the stake to creator
     */
    function cancelGame(uint256 gameId) external validGame(gameId) {
        Game storage game = games[gameId];

        // Only the creator (players[0]) can cancel
        if (msg.sender != game.players[0]) revert NotAPlayer();

        // Can only cancel if game hasn't started (no second player)
        if (game.players[1] != address(0)) revert GameAlreadyStarted();

        // Refund the stake to creator
        uint256 stakeToRefund = game.stakeAmount;

        // Clear the game state - this effectively cancels the game
        game.players[0] = address(0);
        game.boardCommitments[0] = bytes32(0);
        game.stateCommitments[0] = bytes32(0);
        game.prizePool = 0;
        game.stakeAmount = 0;

        payable(msg.sender).transfer(stakeToRefund);

        emit GameEnded(gameId, address(0)); // address(0) indicates cancelled
    }

    /**
     * @dev Launch initial attack (only when no attack is pending)
     */
    function attack(uint256 gameId, uint8 targetX, uint8 targetY) external gameActive(gameId) onlyPlayer(gameId) {
        Game storage game = games[gameId];

        // Only allow attacks when no shot is pending (first move only)
        if (game.lastPlayer != address(0)) revert InvalidMove();

        // First move - starting player attacks first
        address nextAttacker = game.players[game.startingPlayer];
        if (msg.sender != nextAttacker) revert NotYourTurn();
        if (targetX >= 10 || targetY >= 10) revert InvalidCoordinates();

        // Set shot coordinates
        game.lastShotX = targetX;
        game.lastShotY = targetY;
        game.lastPlayer = msg.sender;

        emit ShotFired(gameId, msg.sender, targetX, targetY);
    }

    /**
     * @dev Respond to attack with proof and launch counter-attack
     */
    function respondAndCounter(uint256 gameId, ImpactProof memory impactProof, uint8 counterX, uint8 counterY)
        external
        gameActive(gameId)
        onlyPlayer(gameId)
    {
        Game storage game = games[gameId];

        // Must have a shot to respond to
        if (game.lastPlayer == address(0)) revert InvalidMove();

        // Responder is the other player (not the one who just attacked)
        address responder = getOtherPlayer(gameId, game.lastPlayer);
        if (msg.sender != responder) revert NotYourTurn();

        // Verify proof coordinates match the pending shot
        if (impactProof.publicInputs[1] != game.lastShotX || impactProof.publicInputs[2] != game.lastShotY) {
            revert WrongCoordinates();
        }

        // Check if this position has already been shot by the attacker
        uint8 attackerIndex = getPlayerIndex(gameId, game.lastPlayer);
        if (hasBeenShot(gameId, attackerIndex, game.lastShotX, game.lastShotY)) {
            revert AlreadyShot();
        }

        // Verify zkSNARK proof for the impact
        if (!SHOT_VERIFIER.verifyProof(impactProof.piA, impactProof.piB, impactProof.piC, impactProof.publicInputs)) {
            revert InvalidShotProof();
        }

        // Extract impact result from proof
        Impact result = Impact(impactProof.publicInputs[3]);

        // Verify the previous state commitment matches defender's current commitment
        uint8 defenderIndex = (responder == game.players[0]) ? 0 : 1;
        if (uint256(game.stateCommitments[defenderIndex]) != impactProof.publicInputs[0]) {
            revert CommitmentMismatch();
        }

        // Verify the board commitment matches defender's commitment (5th public input)
        if (uint256(game.boardCommitments[defenderIndex]) != impactProof.publicInputs[5]) {
            revert CommitmentMismatch();
        }

        // Update defender's state commitment with the new commitment from proof
        game.stateCommitments[defenderIndex] = bytes32(impactProof.publicInputs[6]);

        // Record this shot in the attacker's shot grid
        recordShot(gameId, attackerIndex, game.lastShotX, game.lastShotY);

        // Emit impact report
        emit ImpactReported(gameId, msg.sender, result);

        // Check if game is over (all ships sunk)
        if (result == Impact.SUNK && isGameOver(impactProof.publicInputs)) {
            // Attacker (lastPlayer) wins - defender has no ships left
            address winner = game.lastPlayer;
            game.winner = winner;

            // Winner gets entire prize pool
            uint256 totalPayout = game.prizePool;
            game.prizePool = 0;
            payable(winner).transfer(totalPayout);

            emit GameEnded(gameId, winner);
            return;
        }

        // Validate counter-attack coordinates
        if (counterX >= 10 || counterY >= 10) revert InvalidCoordinates();

        // Update game state: responder becomes the new attacker
        game.lastPlayer = responder;
        game.lastShotX = counterX;
        game.lastShotY = counterY;

        // Emit counter-attack
        emit ShotFired(gameId, msg.sender, counterX, counterY);
    }

    // Internal functions

    /**
     * @dev Randomize which player goes first (0 or 1)
     */
    function randomPlayer(uint256 gameId) internal view returns (uint256) {
        // Use blockhash + block timestamp + game ID + player addresses as entropy source
        bytes32 entropy = keccak256(
            abi.encodePacked(
                blockhash(block.number - 1), block.timestamp, gameId, games[gameId].players[0], games[gameId].players[1]
            )
        );
        return uint256(entropy) % 2;
    }

    function isPlayer(Game storage game, address addr) internal view returns (bool) {
        return game.players[0] == addr || game.players[1] == addr;
    }

    function isGameOver(uint256[8] memory publicInputs) internal pure returns (bool) {
        // publicInputs[7] contains the remaining ships count after this shot
        // publicInputs = [previousCommitment, targetX, targetY, claimedResult, claimedShipId, boardCommitment, newCommitment, remainingShips]
        // Game is over when remaining ships = 0
        uint256 remainingShips = publicInputs[7];
        return remainingShips == 0;
    }

    // View functions - minimal state queries

    function getGameInfo(uint256 gameId) external view returns (Game memory) {
        return games[gameId];
    }

    // Helper functions to check game state
    function isGameValid(uint256 gameId) public view returns (bool) {
        return games[gameId].players[0] != address(0);
    }

    function isGameStarted(uint256 gameId) public view returns (bool) {
        return games[gameId].players[1] != address(0);
    }

    function isGameEnded(uint256 gameId) public view returns (bool) {
        return games[gameId].winner != address(0);
    }

    function getOtherPlayer(uint256 gameId, address player) public view returns (address) {
        if (player == games[gameId].players[0]) return games[gameId].players[1];
        if (player == games[gameId].players[1]) return games[gameId].players[0];
        revert InvalidPlayer();
    }

    // Shot grid management functions
    function hasBeenShot(uint256 gameId, uint8 playerIndex, uint8 x, uint8 y) internal view returns (bool) {
        uint256 position = uint256(y) * 10 + uint256(x); // 0-99
        return (games[gameId].shotGrids[playerIndex] >> position) & 1 == 1;
    }

    function recordShot(uint256 gameId, uint8 playerIndex, uint8 x, uint8 y) internal {
        uint256 position = uint256(y) * 10 + uint256(x);
        games[gameId].shotGrids[playerIndex] |= (1 << position);
    }

    function getPlayerIndex(uint256 gameId, address player) internal view returns (uint8) {
        if (player == games[gameId].players[0]) return 0;
        if (player == games[gameId].players[1]) return 1;
        revert InvalidPlayer();
    }

    // Modifiers for game state validation
    modifier validGame(uint256 gameId) {
        if (!isGameValid(gameId)) revert InvalidGameId();
        _;
    }

    modifier gameNotFull(uint256 gameId) {
        if (games[gameId].players[1] != address(0)) revert GameLobbyFull();
        _;
    }

    modifier correctStake(uint256 gameId) {
        if (msg.value != games[gameId].stakeAmount) revert InsufficientStake();
        _;
    }

    modifier notSamePlayer(uint256 gameId) {
        if (games[gameId].players[0] == msg.sender) revert PlayerAlreadyInGame();
        _;
    }

    modifier gameStarted(uint256 gameId) {
        if (!isGameStarted(gameId)) revert GameNotStarted();
        _;
    }

    modifier gameNotEnded(uint256 gameId) {
        if (isGameEnded(gameId)) revert GameAlreadyEnded();
        _;
    }

    modifier gameActive(uint256 gameId) {
        if (!isGameStarted(gameId)) revert GameNotStarted();
        if (isGameEnded(gameId)) revert GameAlreadyEnded();
        _;
    }

    modifier onlyPlayer(uint256 gameId) {
        if (!isPlayer(games[gameId], msg.sender)) revert NotAPlayer();
        _;
    }
}
