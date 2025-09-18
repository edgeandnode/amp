// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Battleship} from "../../src/Battleship.sol";

library BoardLibrary {
    uint8 public constant HORIZONTAL = 0;
    uint8 public constant VERTICAL = 1;

    struct BoardConfig {
        uint8[3] carrier;
        uint8[3] battleship;
        uint8[3] cruiser;
        uint8[3] submarine;
        uint8[3] destroyer;
        uint256 salt;
    }

    struct BoardState {
        address player;
        uint256 gameId;
        uint8[3][5] ships;
        uint8[5] previousHitCounts;
        uint256 previousCommitment;
        uint256 boardCommitment;
        uint256 salt;
    }

    /**
     * @dev Standard board configuration - ships placed in a typical pattern
     * Layout:
     * - Carrier (5): Horizontal at (0,0) to (4,0)
     * - Battleship (4): Horizontal at (0,2) to (3,2)
     * - Cruiser (3): Horizontal at (0,4) to (2,4)
     * - Submarine (3): Horizontal at (0,6) to (2,6)
     * - Destroyer (2): Horizontal at (0,8) to (1,8)
     */
    function standardBoard() internal pure returns (BoardConfig memory) {
        return BoardConfig({
            carrier: [0, 0, HORIZONTAL], // (0,0) to (4,0)
            battleship: [0, 2, HORIZONTAL], // (0,2) to (3,2)
            cruiser: [0, 4, HORIZONTAL], // (0,4) to (2,4)
            submarine: [0, 6, HORIZONTAL], // (0,6) to (2,6)
            destroyer: [0, 8, HORIZONTAL], // (0,8) to (1,8)
            salt: 123456789
        });
    }

    /**
     * @dev Corners board configuration - ships placed in corners and edges
     * Layout:
     * - Carrier (5): Vertical at (0,0) to (0,4)
     * - Battleship (4): Horizontal at (6,0) to (9,0)
     * - Cruiser (3): Vertical at (9,2) to (9,4)
     * - Submarine (3): Horizontal at (0,9) to (2,9)
     * - Destroyer (2): Vertical at (5,7) to (5,8)
     */
    function cornersBoard() internal pure returns (BoardConfig memory) {
        return BoardConfig({
            carrier: [0, 0, VERTICAL], // (0,0) to (0,4)
            battleship: [6, 0, HORIZONTAL], // (6,0) to (9,0)
            cruiser: [9, 2, VERTICAL], // (9,2) to (9,4)
            submarine: [0, 9, HORIZONTAL], // (0,9) to (2,9)
            destroyer: [5, 7, VERTICAL], // (5,7) to (5,8)
            salt: 987654321
        });
    }

    /**
     * @dev Compact board configuration - all ships close together in center
     * Layout:
     * - Carrier (5): Horizontal at (2,3) to (6,3)
     * - Battleship (4): Vertical at (3,0) to (3,3)
     * - Cruiser (3): Horizontal at (4,5) to (6,5)
     * - Submarine (3): Vertical at (7,2) to (7,4)
     * - Destroyer (2): Horizontal at (1,1) to (2,1)
     */
    function compactBoard() internal pure returns (BoardConfig memory) {
        return BoardConfig({
            carrier: [2, 3, HORIZONTAL], // (2,3) to (6,3)
            battleship: [3, 0, VERTICAL], // (3,0) to (3,3)
            cruiser: [4, 5, HORIZONTAL], // (4,5) to (6,5)
            submarine: [7, 2, VERTICAL], // (7,2) to (7,4)
            destroyer: [1, 1, HORIZONTAL], // (1,1) to (2,1)
            salt: 456789123
        });
    }

    /**
     * @dev Spread board configuration - ships maximally separated
     * Layout:
     * - Carrier (5): Vertical at (0,0) to (0,4)
     * - Battleship (4): Horizontal at (6,0) to (9,0)
     * - Cruiser (3): Vertical at (2,6) to (2,8)
     * - Submarine (3): Horizontal at (6,6) to (8,6)
     * - Destroyer (2): Vertical at (9,8) to (9,9)
     */
    function spreadBoard() internal pure returns (BoardConfig memory) {
        return BoardConfig({
            carrier: [0, 0, VERTICAL], // (0,0) to (0,4)
            battleship: [6, 0, HORIZONTAL], // (6,0) to (9,0)
            cruiser: [2, 6, VERTICAL], // (2,6) to (2,8)
            submarine: [6, 6, HORIZONTAL], // (6,6) to (8,6)
            destroyer: [9, 8, VERTICAL], // (9,8) to (9,9)
            salt: 789123456
        });
    }

    /**
     * @dev Quick victory board - designed for fast elimination testing
     * All ships clustered in one area for quick discovery and sinking
     * Layout:
     * - Carrier (5): Horizontal at (0,0) to (4,0)
     * - Battleship (4): Horizontal at (0,1) to (3,1)
     * - Cruiser (3): Horizontal at (0,2) to (2,2)
     * - Submarine (3): Horizontal at (0,3) to (2,3)
     * - Destroyer (2): Horizontal at (0,4) to (1,4)
     */
    function quickVictoryBoard() internal pure returns (BoardConfig memory) {
        return BoardConfig({
            carrier: [0, 0, HORIZONTAL], // (0,0) to (4,0)
            battleship: [0, 1, HORIZONTAL], // (0,1) to (3,1)
            cruiser: [0, 2, HORIZONTAL], // (0,2) to (2,2)
            submarine: [0, 3, HORIZONTAL], // (0,3) to (2,3)
            destroyer: [0, 4, HORIZONTAL], // (0,4) to (1,4)
            salt: 111111111
        });
    }

    /**
     * @dev Get ship configuration with custom salt
     * Useful for creating multiple games with same layout but different commitments
     */
    function standardBoardWithSalt(uint256 customSalt) internal pure returns (BoardConfig memory) {
        BoardConfig memory config = standardBoard();
        config.salt = customSalt;
        return config;
    }

    function cornersBoardWithSalt(uint256 customSalt) internal pure returns (BoardConfig memory) {
        BoardConfig memory config = cornersBoard();
        config.salt = customSalt;
        return config;
    }

    function compactBoardWithSalt(uint256 customSalt) internal pure returns (BoardConfig memory) {
        BoardConfig memory config = compactBoard();
        config.salt = customSalt;
        return config;
    }

    function spreadBoardWithSalt(uint256 customSalt) internal pure returns (BoardConfig memory) {
        BoardConfig memory config = spreadBoard();
        config.salt = customSalt;
        return config;
    }

    function quickVictoryBoardWithSalt(uint256 customSalt) internal pure returns (BoardConfig memory) {
        BoardConfig memory config = quickVictoryBoard();
        config.salt = customSalt;
        return config;
    }

    /**
     * @dev Get hit counts array for tracking ship damage
     * Returns array initialized to all zeros (no hits)
     */
    function noHits() internal pure returns (uint8[5] memory) {
        return [0, 0, 0, 0, 0];
    }

    /**
     * @dev Helper to create hit counts array with specific values
     */
    function hitCounts(uint8 carrier, uint8 battleship, uint8 cruiser, uint8 submarine, uint8 destroyer)
        internal
        pure
        returns (uint8[5] memory)
    {
        return [carrier, battleship, cruiser, submarine, destroyer];
    }

    function shipHitpoints(uint8 shipId) internal pure returns (uint8) {
        require(shipId <= 4, "Invalid ship identifier");

        if (shipId == 0) {
            return 5;
        } else if (shipId == 1) {
            return 4;
        } else if (shipId == 2) {
            return 3;
        } else if (shipId == 3) {
            return 3;
        } else {
            return 2;
        }
    }

    function orientationName(uint8 orientation) internal pure returns (string memory) {
        require(orientation == HORIZONTAL || orientation == VERTICAL, "Invalid orientation");

        if (orientation == HORIZONTAL) {
            return "horizontal";
        } else {
            return "vertical";
        }
    }

    function shipName(uint8 shipId) internal pure returns (string memory) {
        require(shipId <= 4, "Invalid ship identifier");

        if (shipId == 0) {
            return "carrier";
        } else if (shipId == 1) {
            return "battleship";
        } else if (shipId == 2) {
            return "cruiser";
        } else if (shipId == 3) {
            return "submarine";
        } else {
            return "destroyer";
        }
    }

    function impactName(Battleship.Impact impact) internal pure returns (string memory) {
        if (impact == Battleship.Impact.MISS) {
            return "miss";
        } else if (impact == Battleship.Impact.HIT) {
            return "hit";
        } else {
            return "sunk";
        }
    }
}
