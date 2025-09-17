// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {Battleship} from "../../src/Battleship.sol";
import {Groth16Verifier as BoardVerifier} from "../../src/BoardVerifier.sol";
import {Groth16Verifier as ImpactVerifier} from "../../src/ImpactVerifier.sol";
import {BattleshipCircuitProver} from "../helpers/BattleshipCircuitProver.sol";
import {BattleshipTestHelper} from "../helpers/BattleshipTestHelper.sol";

contract FullGameTest is BattleshipTestHelper {
    BattleshipCircuitProver public prover;

    function setUp() public {
        address boardVerifier = address(new BoardVerifier());
        address impactVerifier = address(new ImpactVerifier());
        prover = new BattleshipCircuitProver(boardVerifier, impactVerifier);
        bs = new Battleship(boardVerifier, impactVerifier);

        vm.deal(alice, 10 ether);
        vm.deal(bob, 10 ether);
        vm.deal(charlie, 10 ether);
    }

    function createGame(
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) public returns (uint256) {
        return bs.createGame{value: 0 ether}(
            prover.requestBoardProof({
                carrier: carrier,
                battleship: battleship,
                cruiser: cruiser,
                submarine: submarine,
                destroyer: destroyer,
                salt: salt
            })
        );
    }

    function joinGame(
        uint256 gameId,
        uint8[3] memory carrier,
        uint8[3] memory battleship,
        uint8[3] memory cruiser,
        uint8[3] memory submarine,
        uint8[3] memory destroyer,
        uint256 salt
    ) public {
        bs.joinGame{value: 0 ether}(
            gameId,
            prover.requestBoardProof({
                carrier: carrier,
                battleship: battleship,
                cruiser: cruiser,
                submarine: submarine,
                destroyer: destroyer,
                salt: salt
            })
        );
    }

    function test_BasicGameplay() public {
        Battleship.ImpactProof memory proof;

        vm.startPrank(alice);
        uint256 gameId = createGame({
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 12345
        });

        vm.startPrank(bob);
        joinGame({
            gameId: gameId,
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 54321
        });

        setStartingPlayer(gameId, alice);

        vm.startPrank(alice);
        bs.attack(gameId, 0, 0);

        proof = prover.requestImpactProof({
            previousCommitment: uint256(bs.getStateCommitment(gameId, bob)),
            targetX: 0,
            targetY: 0,
            claimedResult: uint8(Battleship.Impact.HIT),
            claimedShipId: 255,
            boardCommitment: uint256(bs.getBoardCommitment(gameId, bob)),
            ships: [[0, 0, 0], [0, 1, 0], [0, 2, 0], [0, 3, 0], [0, 4, 0]],
            previousHitCounts: [0, 0, 0, 0, 0],
            salt: 54321
        });

        vm.startPrank(bob);
        bs.respondAndCounter(gameId, proof, 0, 0);
    }
}
