// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {Battleship} from "../src/Battleship.sol";
import {Groth16Verifier as BoardVerifier} from "../src/BattleshipBoardVerifier.sol";
import {Groth16Verifier as ShotVerifier} from "../src/BattleshipShotVerifier.sol";
import {BattleshipFFIHelper} from "./helpers/BattleshipFFIHelper.sol";

/**
 * @title BattleshipFFIHelperTest
 * @dev Test using the reusable FFI helper utilities
 */
contract BattleshipFFIHelperTest is Test {
    Battleship public battleship;
    BoardVerifier public boardVerifier;
    ShotVerifier public shotVerifier;
    BattleshipFFIHelper public ffiHelper;

    function setUp() public {
        // Deploy real verifier contracts
        boardVerifier = new BoardVerifier();
        shotVerifier = new ShotVerifier();

        // Deploy battleship contract with real verifiers
        battleship = new Battleship(address(boardVerifier), address(shotVerifier));

        // Deploy FFI helper
        ffiHelper = new BattleshipFFIHelper();
    }

    /**
     * @dev Test board proof generation using helper
     */
    function test_FFIHelper_BoardProof() public {
        // Generate board proof response using helper
        string memory response = ffiHelper.generateBoardProofResponse();

        // Validate we got a valid response
        assertTrue(bytes(response).length > 0, "Should receive response");
        assertTrue(ffiHelper.contains(response, '"success":true'), "Should be successful");
        assertTrue(ffiHelper.contains(response, '"publicInputs":['), "Should contain public inputs");

        console.log("Board proof response:", response);
        console.log("SUCCESS: Real board proof generated via helper");
    }

    /**
     * @dev Test shot proof generation using helper
     */
    function test_FFIHelper_ShotProof() public {
        // First get board proof to extract commitment values (in production, parse this response)
        ffiHelper.generateBoardProofResponse();

        // Use known commitment values (in production, these would be parsed from boardResponse)
        string memory boardCommitment = "5862941505087434965316664484210928782338864322398636804362927351039075444662";
        string memory initialStateCommitment =
            "4390886961511024917197089306016340810950581231437908102996443113502996587142";

        // Test shot at water position
        uint8 targetX = 7;
        uint8 targetY = 8;

        // Verify it's a water position
        ffiHelper.assertShotHitsWater(targetX, targetY);

        // Generate shot proof using helper
        string memory shotResponse =
            ffiHelper.generateShotProofResponse(targetX, targetY, boardCommitment, initialStateCommitment);

        // Validate we got a valid response
        assertTrue(bytes(shotResponse).length > 0, "Should receive response");
        assertTrue(ffiHelper.contains(shotResponse, '"success":true'), "Should be successful");
        assertTrue(ffiHelper.contains(shotResponse, '"publicInputs":['), "Should contain public inputs");

        console.log("Shot proof response:", shotResponse);
        console.log("SUCCESS: Real shot proof generated via helper");
    }

    /**
     * @dev Test multiple water shots using helper
     */
    function test_FFIHelper_MultipleWaterShots() public {
        // Get commitment values once
        string memory boardCommitment = "5862941505087434965316664484210928782338864322398636804362927351039075444662";
        string memory initialStateCommitment =
            "4390886961511024917197089306016340810950581231437908102996443113502996587142";

        (uint8[3] memory waterX, uint8[3] memory waterY) = ffiHelper.getWaterPositions();

        for (uint256 i = 0; i < 3; i++) {
            uint8 x = waterX[i];
            uint8 y = waterY[i];

            // Verify it's water
            ffiHelper.assertShotHitsWater(x, y);

            // Generate proof response
            string memory response = ffiHelper.generateShotProofResponse(x, y, boardCommitment, initialStateCommitment);

            // Validate
            assertTrue(ffiHelper.contains(response, '"success":true'), "Should be successful");
            assertTrue(ffiHelper.contains(response, '"publicInputs":['), "Should contain public inputs");

            console.log("Real shot proof generated for water position", x, y);
        }

        console.log("SUCCESS: Multiple real water shot proofs generated via helper");
    }

    /**
     * @dev Test helper utility functions
     */
    function test_FFIHelper_Utilities() public {
        // Test string conversion
        assertEq(ffiHelper.uint8ToString(0), "0");
        assertEq(ffiHelper.uint8ToString(5), "5");
        assertEq(ffiHelper.uint8ToString(42), "42");
        assertEq(ffiHelper.uint8ToString(255), "255");

        // Test string contains
        assertTrue(ffiHelper.contains("hello world", "world"));
        assertFalse(ffiHelper.contains("hello", "world"));
        assertTrue(ffiHelper.contains("test", "test"));

        // Test JSON building with commitment values
        string memory boardCommitment = "test_board_commitment";
        string memory previousCommitment = "test_previous_commitment";
        string memory json = ffiHelper.buildShotProofRequest(5, 7, 0, boardCommitment, previousCommitment);
        assertTrue(ffiHelper.contains(json, '"targetX":5'));
        assertTrue(ffiHelper.contains(json, '"targetY":7'));
        assertTrue(ffiHelper.contains(json, '"claimedResult":0'));
        assertTrue(ffiHelper.contains(json, boardCommitment));
        assertTrue(ffiHelper.contains(json, previousCommitment));

        console.log("SUCCESS: Helper utility functions validated");
    }

    /**
     * @dev Test custom board configuration
     */
    function test_FFIHelper_CustomBoard() public {
        // Create a custom board layout - ships in different positions
        uint8[3] memory customCarrier = [1, 1, 1]; // Vertical at (1,1)-(1,5)
        uint8[3] memory customBattleship = [3, 0, 0]; // Horizontal at (3,0)-(6,0)
        uint8[3] memory customCruiser = [5, 5, 1]; // Vertical at (5,5)-(5,7)
        uint8[3] memory customSubmarine = [0, 8, 0]; // Horizontal at (0,8)-(2,8)
        uint8[3] memory customDestroyer = [8, 2, 1]; // Vertical at (8,2)-(8,3)
        uint256 customSalt = 54321;

        // Generate board proof with custom configuration
        string memory response = ffiHelper.generateBoardProofResponse(
            customCarrier, customBattleship, customCruiser, customSubmarine, customDestroyer, customSalt
        );

        // Validate we got a valid response
        assertTrue(bytes(response).length > 0, "Should receive response");
        assertTrue(ffiHelper.contains(response, '"success":true'), "Should be successful");
        assertTrue(ffiHelper.contains(response, '"publicInputs":['), "Should contain public inputs");

        console.log("Custom board proof response:", response);
        console.log("SUCCESS: Custom board configuration proof generated");
    }

    /**
     * @dev Test building board proof request JSON
     */
    function test_FFIHelper_BuildBoardRequest() public view {
        uint8[3] memory carrier = [2, 3, 0];
        uint8[3] memory battleshipShip = [4, 5, 1];
        uint8[3] memory cruiser = [6, 7, 0];
        uint8[3] memory submarine = [1, 9, 1];
        uint8[3] memory destroyer = [8, 1, 0];
        uint256 salt = 99999;

        string memory json =
            ffiHelper.buildBoardProofRequest(carrier, battleshipShip, cruiser, submarine, destroyer, salt);

        // Validate JSON structure
        assertTrue(ffiHelper.contains(json, '"type":"board"'), "Should contain type");
        assertTrue(ffiHelper.contains(json, '"carrier":[2,3,0]'), "Should contain carrier config");
        assertTrue(ffiHelper.contains(json, '"battleship":[4,5,1]'), "Should contain battleship config");
        assertTrue(ffiHelper.contains(json, '"cruiser":[6,7,0]'), "Should contain cruiser config");
        assertTrue(ffiHelper.contains(json, '"submarine":[1,9,1]'), "Should contain submarine config");
        assertTrue(ffiHelper.contains(json, '"destroyer":[8,1,0]'), "Should contain destroyer config");
        assertTrue(ffiHelper.contains(json, '"salt":99999'), "Should contain salt");

        console.log("Built board JSON:", json);
        console.log("SUCCESS: Board request JSON building validated");
    }
}
