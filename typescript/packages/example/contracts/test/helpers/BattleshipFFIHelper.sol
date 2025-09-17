// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {Battleship} from "../../src/Battleship.sol";

/**
 * @title BattleshipFFIHelper
 * @dev Reusable utilities for FFI-based ZK proof generation in Battleship tests
 */
contract BattleshipFFIHelper is Test {
    // These values must be extracted from board proof responses - no hardcoding allowed
    // Use generateBoardProofResponse() to get actual values

    /**
     * @dev Generate and validate board proof via FFI with custom board configuration
     */
    function generateBoardProofResponse(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) public returns (string memory) {
        string memory request = buildBoardProofRequest(carrier, battleship, cruiser, submarine, destroyer, salt);
        string memory response = callFFI(request);

        require(validateProofResponse(response), "Board proof generation failed");
        return response;
    }

    /**
     * @dev Generate board proof with default ship arrangement (for backward compatibility)
     */
    function generateBoardProofResponse() public returns (string memory) {
        uint8[3] memory carrier = [0, 0, 0];
        uint8[3] memory battleship = [0, 1, 0];
        uint8[3] memory cruiser = [0, 2, 0];
        uint8[3] memory submarine = [0, 3, 0];
        uint8[3] memory destroyer = [0, 4, 0];
        uint256 salt = 12345;

        return generateBoardProofResponse(carrier, battleship, cruiser, submarine, destroyer, salt);
    }

    /**
     * @dev Generate shot proof response using provided commitment values
     * NOTE: Caller must extract commitment values from board proof response
     */
    function generateShotProofResponse(
        uint8 targetX,
        uint8 targetY,
        string memory boardCommitment,
        string memory previousCommitment
    ) public returns (string memory) {
        string memory request = buildShotProofRequest(targetX, targetY, 0, boardCommitment, previousCommitment);
        string memory response = callFFI(request);

        require(validateProofResponse(response), "Shot proof generation failed");
        return response;
    }

    /**
     * @dev Build board proof request JSON with custom ship configuration
     */
    function buildBoardProofRequest(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) public pure returns (string memory) {
        return string(
            abi.encodePacked(
                '{"type":"board","input":{"carrier":[',
                uint8ToString(carrier[0]),
                ",",
                uint8ToString(carrier[1]),
                ",",
                uint8ToString(carrier[2]),
                '],"battleship":[',
                uint8ToString(battleship[0]),
                ",",
                uint8ToString(battleship[1]),
                ",",
                uint8ToString(battleship[2]),
                '],"cruiser":[',
                uint8ToString(cruiser[0]),
                ",",
                uint8ToString(cruiser[1]),
                ",",
                uint8ToString(cruiser[2]),
                '],"submarine":[',
                uint8ToString(submarine[0]),
                ",",
                uint8ToString(submarine[1]),
                ",",
                uint8ToString(submarine[2]),
                '],"destroyer":[',
                uint8ToString(destroyer[0]),
                ",",
                uint8ToString(destroyer[1]),
                ",",
                uint8ToString(destroyer[2]),
                '],"salt":',
                uint256ToString(salt),
                "}}"
            )
        );
    }

    /**
     * @dev Build shot proof request JSON with dynamic commitment values
     */
    function buildShotProofRequest(
        uint8 targetX,
        uint8 targetY,
        uint8 claimedResult,
        string memory boardCommitment,
        string memory previousCommitment
    ) public pure returns (string memory) {
        return string(
            abi.encodePacked(
                '{"type":"shot","input":{"previousCommitment":"',
                previousCommitment,
                '","targetX":',
                uint8ToString(targetX),
                ',"targetY":',
                uint8ToString(targetY),
                ',"claimedResult":',
                uint8ToString(claimedResult),
                ',"claimedShipId":255,"boardCommitment":"',
                boardCommitment,
                '","ships":[[0,0,0],[0,1,0],[0,2,0],[0,3,0],[0,4,0]],"previousHitCounts":[0,0,0,0,0],"salt":12345}}'
            )
        );
    }

    /**
     * @dev Call FFI script with request as CLI argument
     */
    function callFFI(string memory request) public returns (string memory) {
        string[] memory cmd = new string[](6);
        cmd[0] = "node";
        cmd[1] = "--disable-warning=ExperimentalWarning";
        cmd[2] = "--experimental-transform-types";
        cmd[3] = "../test/ffi.ts";
        cmd[4] = request;

        bytes memory result = vm.ffi(cmd);
        return string(result);
    }

    /**
     * @dev Validate proof response structure
     */
    function validateProofResponse(string memory response) public pure returns (bool) {
        return bytes(response).length > 100 && contains(response, '"success":true') && contains(response, '"piA":')
            && contains(response, '"piB":') && contains(response, '"piC":') && contains(response, '"publicInputs":');
    }

    /**
     * @dev Check if string contains substring
     */
    function contains(string memory source, string memory target) public pure returns (bool) {
        bytes memory sourceBytes = bytes(source);
        bytes memory targetBytes = bytes(target);

        if (targetBytes.length > sourceBytes.length) return false;

        for (uint256 i = 0; i <= sourceBytes.length - targetBytes.length; i++) {
            bool found = true;
            for (uint256 j = 0; j < targetBytes.length; j++) {
                if (sourceBytes[i + j] != targetBytes[j]) {
                    found = false;
                    break;
                }
            }
            if (found) return true;
        }
        return false;
    }

    /**
     * @dev Convert uint8 to string
     */
    function uint8ToString(uint8 value) public pure returns (string memory) {
        if (value == 0) return "0";

        uint8 temp = value;
        uint8 digits;
        while (temp != 0) {
            digits++;
            temp /= 10;
        }

        bytes memory buffer = new bytes(digits);
        while (value != 0) {
            digits -= 1;
            buffer[digits] = bytes1(uint8(48 + uint8(value % 10)));
            value /= 10;
        }

        return string(buffer);
    }

    /**
     * @dev Convert uint256 to string
     */
    function uint256ToString(uint256 value) public pure returns (string memory) {
        if (value == 0) return "0";

        uint256 temp = value;
        uint256 digits;
        while (temp != 0) {
            digits++;
            temp /= 10;
        }

        bytes memory buffer = new bytes(digits);
        while (value != 0) {
            digits -= 1;
            buffer[digits] = bytes1(uint8(48 + uint8(value % 10)));
            value /= 10;
        }

        return string(buffer);
    }

    /**
     * @dev Assert that shot hits water (for testing misses)
     */
    function assertShotHitsWater(uint8 x, uint8 y) public pure {
        // Board layout: carrier(0,0)-(4,0), battleship(0,1)-(3,1), cruiser(0,2)-(2,2), submarine(0,3)-(2,3), destroyer(0,4)-(1,4)
        require(
            ! // carrier
                // battleship
                // cruiser
                // submarine
            ((y == 0 && x <= 4) || (y == 1 && x <= 3) || (y == 2 && x <= 2) || (y == 3 && x <= 2) || (y == 4 && x <= 1)), // destroyer
            "Shot should hit water, not a ship"
        );
    }

    /**
     * @dev Get known water positions for testing
     */
    function getWaterPositions() public pure returns (uint8[3] memory x, uint8[3] memory y) {
        x = [5, 7, 9];
        y = [6, 7, 9];
    }
}
