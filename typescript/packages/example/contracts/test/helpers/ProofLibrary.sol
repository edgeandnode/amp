// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Vm} from "forge-std/Vm.sol";
import {Battleship} from "../../src/Battleship.sol";
import {BoardLibrary} from "./BoardLibrary.sol";

library ProofLibrary {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

    uint256 public constant MOCK_BOARD_COMMITMENT = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;
    uint256 public constant MOCK_STATE_COMMITMENT = 0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321;

    enum ProofType {
        BOARD,
        IMPACT
    }

    /**
     * @dev Create a real board proof
     */
    function createBoardProof(BoardLibrary.BoardConfig memory config) internal returns (Battleship.BoardProof memory) {
        return createBoardProof(
            config.carrier, config.battleship, config.cruiser, config.submarine, config.destroyer, config.salt
        );
    }

    /**
     * @dev Create a real board proof
     */
    function createBoardProof(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) internal returns (Battleship.BoardProof memory) {
        // Create input object first
        string memory input = "";

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
        vm.serializeUint(input, "carrier", carrierArray);
        vm.serializeUint(input, "battleship", battleshipArray);
        vm.serializeUint(input, "cruiser", cruiserArray);
        vm.serializeUint(input, "submarine", submarineArray);
        vm.serializeUint(input, "destroyer", destroyerArray);

        string memory result = createProof(ProofType.BOARD, vm.serializeUint(input, "salt", salt));
        return parseBoardProofResponse(result);
    }

    /**
     * @dev Create a real impact proof
     */
    function createImpactProof(
        uint256 previousCommitment,
        uint8 targetX,
        uint8 targetY,
        uint8 claimedResult,
        uint8 claimedShipId,
        uint256 boardCommitment,
        uint8[3][5] memory ships,
        uint8[5] memory previousHitCounts,
        uint256 salt
    ) internal returns (Battleship.ImpactProof memory) {
        string memory result = createProof(
            ProofType.IMPACT,
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
     * @dev Helper to format ships array as json
     */
    function formatShipsArray(uint8[3][5] memory ships) private pure returns (string memory) {
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
     * @dev Helper to format a single ship array as json
     */
    function formatShip(uint8[3] memory ship) private pure returns (string memory) {
        return string.concat("[", vm.toString(ship[0]), ",", vm.toString(ship[1]), ",", vm.toString(ship[2]), "]");
    }

    /**
     * @dev Helper to format hit counts array as json
     */
    function formatHitCountsArray(uint8[5] memory hitCounts) private pure returns (string memory) {
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
     * @dev Create a real board or impact proof
     */
    function createProof(ProofType proofType, string memory proofRequest) private returns (string memory) {
        string memory script = "../test/ffi.ts";
        string[] memory cmd = new string[](6);
        cmd[0] = "node";
        cmd[1] = "--disable-warning=ExperimentalWarning";
        cmd[2] = "--experimental-transform-types";
        cmd[3] = script;
        cmd[4] = proofType == ProofType.BOARD ? "board" : "impact";
        cmd[5] = proofRequest;

        bytes memory result = vm.ffi(cmd);
        string memory response = string(result);
        require(vm.parseJsonBool(response, ".success"), "Proof creation failed");

        return response;
    }

    /**
     * @dev Parse board proof response into structured data
     */
    function parseBoardProofResponse(string memory response) private pure returns (Battleship.BoardProof memory data) {
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
        private
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
     * @dev Get mock board proof
     */
    function mockBoardProof() public pure returns (Battleship.BoardProof memory) {
        return mockBoardProof(MOCK_BOARD_COMMITMENT, MOCK_STATE_COMMITMENT);
    }

    /**
     * @dev Get mock board proof
     */
    function mockBoardProof(bytes32 boardCommitment, bytes32 stateCommitment)
        public
        pure
        returns (Battleship.BoardProof memory)
    {
        return mockBoardProof(uint256(boardCommitment), uint256(stateCommitment));
    }

    /**
     * @dev Get mock board proof
     */
    function mockBoardProof(uint256 boardCommitment, uint256 stateCommitment)
        public
        pure
        returns (Battleship.BoardProof memory)
    {
        return Battleship.BoardProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicSignals: [boardCommitment, stateCommitment]
        });
    }

    /**
     * @dev Create a mock impact proof
     */
    function mockImpactProof(
        uint256 newCommitment,
        uint256 remainingShips,
        uint256 previousCommitment,
        uint256 targetX,
        uint256 targetY,
        Battleship.Impact claimedResult,
        uint256 claimedShipId,
        uint256 boardCommitment
    ) public pure returns (Battleship.ImpactProof memory) {
        return Battleship.ImpactProof({
            piA: [uint256(1), uint256(2)],
            piB: [[uint256(3), uint256(4)], [uint256(5), uint256(6)]],
            piC: [uint256(7), uint256(8)],
            publicSignals: [
                newCommitment, // newCommitment (index 0) - same for miss
                remainingShips, // remainingShips (index 1)
                previousCommitment, // previousCommitment (index 2)
                targetX, // targetX (index 3)
                targetY, // targetY (index 4)
                uint256(claimedResult), // claimedResult (index 5)
                claimedShipId, // claimedShipId (index 6) - 255 for not sunk
                boardCommitment // boardCommitment (index 7)
            ]
        });
    }
}
