// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {Battleship} from "../../src/Battleship.sol";
import {IImpactVerifier, IBoardVerifier} from "../../src/Battleship.sol";

interface ICircuitProver {
    function requestBoardProof(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) external returns (Battleship.BoardProof memory);

    function requestImpactProof(
        uint256 previousCommitment,
        uint8 targetX,
        uint8 targetY,
        uint8 claimedResult,
        uint8 claimedShipId,
        uint256 boardCommitment,
        uint8[3][5] memory ships,
        uint8[5] memory previousHitCounts,
        uint256 salt
    ) external returns (Battleship.ImpactProof memory);
}

contract BattleshipCircuitProver is Test, ICircuitProver {
    IImpactVerifier public impactVerifier;
    IBoardVerifier public boardVerifier;

    constructor(address _boardVerifier, address _impactVerifier) {
        boardVerifier = IBoardVerifier(_boardVerifier);
        impactVerifier = IImpactVerifier(_impactVerifier);
    }

    /**
     * @dev Create board proof request JSON using proper Foundry serialization
     */
    function requestBoardProof(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) external returns (Battleship.BoardProof memory) {
        // Create input object first
        string memory inputObj = "boardInput";

        // Convert ship arrays to uint256[] for Foundry serialization
        uint256[] memory carrierArray = new uint256[](3);
        uint256[] memory battleshipArray = new uint256[](3);
        uint256[] memory cruiserArray = new uint256[](3);
        uint256[] memory submarineArray = new uint256[](3);
        uint256[] memory destroyerArray = new uint256[](3);

        for (uint256 i = 0; i < 3; i++) {
            carrierArray[i] = uint256(carrier[i]);
            battleshipArray[i] = uint256(battleship[i]);
            cruiserArray[i] = uint256(cruiser[i]);
            submarineArray[i] = uint256(submarine[i]);
            destroyerArray[i] = uint256(destroyer[i]);
        }

        // Serialize input object
        vm.serializeUint(inputObj, "carrier", carrierArray);
        vm.serializeUint(inputObj, "battleship", battleshipArray);
        vm.serializeUint(inputObj, "cruiser", cruiserArray);
        vm.serializeUint(inputObj, "submarine", submarineArray);
        vm.serializeUint(inputObj, "destroyer", destroyerArray);

        string memory result = requestProof("board", vm.serializeUint(inputObj, "salt", salt));
        return parseBoardProofResponse(result);
    }

    /**
     * @dev Create impact proof request JSON using proper Foundry serialization
     */
    function requestImpactProof(
        uint256 previousCommitment,
        uint8 targetX,
        uint8 targetY,
        uint8 claimedResult,
        uint8 claimedShipId,
        uint256 boardCommitment,
        uint8[3][5] memory ships,
        uint8[5] memory previousHitCounts,
        uint256 salt
    ) external returns (Battleship.ImpactProof memory) {
        string memory result = requestProof(
            "impact",
            string.concat(
                "{",
                '"previousCommitment":"',
                vm.toString(previousCommitment),
                '",',
                '"targetX":',
                vm.toString(targetX),
                ",",
                '"targetY":',
                vm.toString(targetY),
                ",",
                '"claimedResult":',
                vm.toString(claimedResult),
                ",",
                '"claimedShipId":',
                vm.toString(claimedShipId),
                ",",
                '"boardCommitment":"',
                vm.toString(boardCommitment),
                '",',
                '"ships":',
                formatShipsArray(ships),
                ",",
                '"previousHitCounts":',
                formatHitCountsArray(previousHitCounts),
                ",",
                '"salt":',
                vm.toString(salt),
                "}"
            )
        );

        return parseImpactProofResponse(result);
    }

    /**
     * @dev Helper to format ships array as proper JSON nested array
     */
    function formatShipsArray(uint8[3][5] memory ships) internal pure returns (string memory) {
        return string.concat(
            "[",
            formatShip(ships[0]),
            ",",
            formatShip(ships[1]),
            ",",
            formatShip(ships[2]),
            ",",
            formatShip(ships[3]),
            ",",
            formatShip(ships[4]),
            "]"
        );
    }

    /**
     * @dev Helper to format a single ship as JSON array
     */
    function formatShip(uint8[3] memory ship) internal pure returns (string memory) {
        return string.concat("[", vm.toString(ship[0]), ",", vm.toString(ship[1]), ",", vm.toString(ship[2]), "]");
    }

    /**
     * @dev Helper to format hit counts array as JSON
     */
    function formatHitCountsArray(uint8[5] memory hitCounts) internal pure returns (string memory) {
        return string.concat(
            "[",
            vm.toString(hitCounts[0]),
            ",",
            vm.toString(hitCounts[1]),
            ",",
            vm.toString(hitCounts[2]),
            ",",
            vm.toString(hitCounts[3]),
            ",",
            vm.toString(hitCounts[4]),
            "]"
        );
    }

    /**
     * @dev Shared FFI helper function
     */
    function requestProof(string memory command, string memory request) internal returns (string memory) {
        string memory script = "../test/ffi.ts";
        string[] memory cmd = new string[](6);
        cmd[0] = "node";
        cmd[1] = "--disable-warning=ExperimentalWarning";
        cmd[2] = "--experimental-transform-types";
        cmd[3] = script;
        cmd[4] = command;
        cmd[5] = request;

        bytes memory result = vm.ffi(cmd);
        string memory response = string(result);
        require(vm.parseJsonBool(response, ".success"), "FFI call failed");

        return response;
    }

    /**
     * @dev Parse board proof response into structured data
     */
    function parseBoardProofResponse(string memory response)
        internal
        pure
        returns (Battleship.BoardProof memory data)
    {
        // Extract proof components
        uint256[] memory piAArray = vm.parseJsonUintArray(response, ".proof.piA");
        data.piA[0] = piAArray[0];
        data.piA[1] = piAArray[1];

        uint256[] memory piB0Array = vm.parseJsonUintArray(response, ".proof.piB[0]");
        uint256[] memory piB1Array = vm.parseJsonUintArray(response, ".proof.piB[1]");
        data.piB[0][0] = piB0Array[0];
        data.piB[0][1] = piB0Array[1];
        data.piB[1][0] = piB1Array[0];
        data.piB[1][1] = piB1Array[1];

        uint256[] memory piCArray = vm.parseJsonUintArray(response, ".proof.piC");
        data.piC[0] = piCArray[0];
        data.piC[1] = piCArray[1];

        uint256[] memory publicSignals = vm.parseJsonUintArray(response, ".proof.publicSignals");
        require(publicSignals.length == 2, "Board proof should have 2 public signals");
        data.publicSignals[0] = publicSignals[0];
        data.publicSignals[1] = publicSignals[1];

        return data;
    }

    /**
     * @dev Parse impact proof response into structured data
     */
    function parseImpactProofResponse(string memory response)
        internal
        pure
        returns (Battleship.ImpactProof memory data)
    {
        // Extract proof components
        uint256[] memory piAArray = vm.parseJsonUintArray(response, ".proof.piA");
        data.piA[0] = piAArray[0];
        data.piA[1] = piAArray[1];

        uint256[] memory piB0Array = vm.parseJsonUintArray(response, ".proof.piB[0]");
        uint256[] memory piB1Array = vm.parseJsonUintArray(response, ".proof.piB[1]");
        data.piB[0][0] = piB0Array[0];
        data.piB[0][1] = piB0Array[1];
        data.piB[1][0] = piB1Array[0];
        data.piB[1][1] = piB1Array[1];

        uint256[] memory piCArray = vm.parseJsonUintArray(response, ".proof.piC");
        data.piC[0] = piCArray[0];
        data.piC[1] = piCArray[1];

        uint256[] memory publicSignals = vm.parseJsonUintArray(response, ".proof.publicSignals");
        require(publicSignals.length == 8, "Shot proof should have 8 public inputs");
        for (uint256 i = 0; i < 8; i++) {
            data.publicSignals[i] = publicSignals[i];
        }

        return data;
    }

    /**
     * @dev Verify board proof with BoardVerifier
     */
    function verifyBoardProof(Battleship.BoardProof memory data) internal view returns (bool) {
        return boardVerifier.verifyProof(data.piA, data.piB, data.piC, data.publicSignals);
    }

    /**
     * @dev Verify impact proof with ImpactVerifier
     */
    function verifyImpactProof(Battleship.ImpactProof memory data) internal view returns (bool) {
        return impactVerifier.verifyProof(data.piA, data.piB, data.piC, data.publicSignals);
    }
}
