// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

import {console} from "forge-std/console.sol";
import {Battleship} from "../src/Battleship.sol";
import {Groth16Verifier as BoardVerifier} from "../src/BoardVerifier.sol";
import {Groth16Verifier as ImpactVerifier} from "../src/ImpactVerifier.sol";
import {ProofLibrary} from "./helpers/ProofLibrary.sol";
import {BoardLibrary} from "./helpers/BoardLibrary.sol";
import {TestBase} from "./helpers/TestBase.sol";

contract BattleshipTest is TestBase {
    function test_BasicGameplay() public {
        // Alice creates a game and bob joins
        BoardLibrary.BoardState memory aliceState =
            createGame(alice, 1 ether, BoardLibrary.standardBoardWithSalt(12345));
        BoardLibrary.BoardState memory bobState =
            joinGame(bob, aliceState.gameId, 1 ether, BoardLibrary.standardBoardWithSalt(54321));

        // Set starting player to make game deterministic
        setStartingPlayer(aliceState.gameId, alice);

        // =======================================================
        // ACT 1: Bob sinks Alice's carrier
        // =======================================================

        // Alice makes the initial attack (0,0) => HIT (carrier)
        initialAttack(aliceState, 0, 0);
        // Bob responds (HIT carrier) and counter attacks (0,0)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 0, 0, 0);
        // Alice responds (HIT carrier) and counter attacks (0,1)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 0, 0, 1);
        // Bob responds (MISS) and counter attacks (1,0)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 1, 0);
        // Alice responds (HIT carrier) and counter attacks (0,2)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 0, 0, 2);
        // Bob responds (HIT battleship) and counter attacks (2,0)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 1, 2, 0);
        // Alice responds (HIT carrier) and counter attacks (0,3)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 0, 0, 3);
        // Bob responds (MISS) and counter attacks (3,0)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 3, 0);
        // Alice responds (HIT battleship) and counter attacks (0,4)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 0, 0, 4);
        // Bob responds (HIT cruiser) and counter attacks (4,0)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 2, 4, 0);
        // Alice responds (SUNK carrier) and counter attacks (0,5)
        aliceState = counterAttack(aliceState, Battleship.Impact.SUNK, 0, 0, 5);

        // =======================================================
        // ACT 2: Bob sinks Alice's battleship
        // =======================================================

        // Bob responds (MISS) and counter attacks (0,2)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 0, 2);
        // Alice responds (HIT battleship) and counter attacks (0,6)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 1, 0, 6);
        // Bob responds (HIT submarine) and counter attacks (1,2)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 3, 1, 2);
        // Alice responds (HIT battleship) and counter attacks (0,7)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 1, 0, 7);
        // Bob responds (MISS) and counter attacks (2,2)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 2, 2);
        // Alice responds (HIT battleship) and counter attacks (0,8)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 1, 0, 8);
        // Bob responds (HIT destroyer) and counter attacks (3,2)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 4, 3, 2);
        // Alice responds (SUNK battleship) and counter attacks (0,9)
        aliceState = counterAttack(aliceState, Battleship.Impact.SUNK, 1, 0, 9);

        // =======================================================
        // ACT 3: Bob sinks Alice's cruiser
        // =======================================================

        // Bob responds (MISS) and counter attacks (0,4)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 0, 4);
        // Alice responds (HIT cruiser) and counter attacks (1,0)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 2, 1, 0);
        // Bob responds (HIT carrier) and counter attacks (1,4)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 0, 1, 4);
        // Alice responds (HIT cruiser) and counter attacks (1,1)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 2, 1, 1);
        // Bob responds (MISS) and counter attacks (2,4)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 2, 4);
        // Alice responds (SUNK cruiser) and counter attacks (1,2)
        aliceState = counterAttack(aliceState, Battleship.Impact.SUNK, 2, 1, 2);

        // =======================================================
        // ACT 4: Bob sinks Alice's submarine
        // =======================================================

        // Bob responds (HIT battleship) and counter attacks (0,6)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 1, 0, 6);
        // Alice responds (HIT submarine) and counter attacks (1,3)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 3, 1, 3);
        // Bob responds (MISS) and counter attacks (1,6)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 1, 6);
        // Alice responds (HIT submarine) and counter attacks (1,4)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 3, 1, 4);
        // Bob responds (HIT cruiser) and counter attacks (2,6)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 2, 2, 6);
        // Alice responds (SUNK submarine) and counter attacks (1,5)
        aliceState = counterAttack(aliceState, Battleship.Impact.SUNK, 3, 1, 5);

        // =======================================================
        // ACT 5: Bob sinks Alice's destroyer (final ship)
        // =======================================================

        // Bob responds (MISS) and counter attacks (0,8)
        bobState = counterAttack(bobState, Battleship.Impact.MISS, 255, 0, 8);
        // Alice responds (HIT destroyer) and counter attacks (1,6)
        aliceState = counterAttack(aliceState, Battleship.Impact.HIT, 4, 1, 6);
        // Bob responds (HIT submarine) and counter attacks (1,8)
        bobState = counterAttack(bobState, Battleship.Impact.HIT, 3, 1, 8);
        // Alice responds (SUNK destroyer) - GAME OVER! Alice has no ships left
        aliceState = counterAttack(aliceState, Battleship.Impact.SUNK, 4, 1, 8);

        // =======================================================
        // GAME OVER
        // =======================================================

        logBoardState(aliceState);
        logBoardState(bobState);
    }
}
