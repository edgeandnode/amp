pragma circom 2.1.0;

include "../node_modules/circomlib/circuits/poseidon.circom";
include "../node_modules/circomlib/circuits/comparators.circom";
include "../node_modules/circomlib/circuits/gates.circom";
include "../node_modules/circomlib/circuits/mux1.circom";
include "../node_modules/circomlib/circuits/mux3.circom";
include "./ship.circom";
include "./utils.circom";

/**
 * @title Battleship Shot Verification Circuit with Commitment Chain
 * @dev Verifies hit/miss claims using commitment chain to track hit counts per ship
 */

/**
 * @dev Check if a coordinate hits any ship
 */
template CoordinateHitsShip(shipLength) {
    signal input targetX;
    signal input targetY;
    signal input shipCoords[shipLength][2];
    signal output isHit;
    signal output hitIndex; // Which cell of the ship was hit (0 if no hit)

    component coordEquals[shipLength];
    signal hits[shipLength];

    for (var i = 0; i < shipLength; i++) {
        coordEquals[i] = CoordEqual();
        coordEquals[i].coord1[0] <== targetX;
        coordEquals[i].coord1[1] <== targetY;
        coordEquals[i].coord2[0] <== shipCoords[i][0];
        coordEquals[i].coord2[1] <== shipCoords[i][1];
        
        hits[i] <== coordEquals[i].equal;
    }

    // Sum all hits - should be 0 or 1
    var hitSum = 0;
    var indexSum = 0;
    for (var i = 0; i < shipLength; i++) {
        hitSum += hits[i];
        indexSum += hits[i] * (i + 1); // Add 1 to avoid 0 index confusion
    }

    // Convert sum to boolean
    component gt = GreaterThan(4);
    gt.in[0] <== hitSum;
    gt.in[1] <== 0;

    isHit <== gt.out;
    hitIndex <== indexSum;
}

/**
 * @dev Main shot verification circuit with commitment chain
 */
template BattleshipShot() {
    // Constants
    var SHIP_LENGTHS[5] = [5, 4, 3, 3, 2];

    // Public inputs
    signal input previousCommitment;    // Previous state commitment
    signal input targetX;               // X coordinate of shot
    signal input targetY;               // Y coordinate of shot
    signal input claimedResult;         // 0=miss, 1=hit, 2=sunk
    signal input claimedShipId;         // 0-4 if sunk, 255 otherwise
    signal input boardCommitment;       // Original board commitment for validation

    // Public outputs (computed by circuit)
    signal output newCommitment;        // New state commitment
    signal output remainingShips;       // Number of ships remaining after this shot

    // Private inputs
    signal input ships[5][3];           // [x, y, orientation] for each ship
    signal input previousHitCounts[5];  // Hit counts before this shot
    signal input salt;                  // Random salt for commitment

    // Step 1: Verify board commitment matches ship positions
    component boardCommitmentHash = Poseidon(16);
    boardCommitmentHash.inputs[0] <== ships[0][0];  // carrier x
    boardCommitmentHash.inputs[1] <== ships[0][1];  // carrier y
    boardCommitmentHash.inputs[2] <== ships[0][2];  // carrier orientation
    boardCommitmentHash.inputs[3] <== ships[1][0];  // battleship x
    boardCommitmentHash.inputs[4] <== ships[1][1];  // battleship y
    boardCommitmentHash.inputs[5] <== ships[1][2];  // battleship orientation
    boardCommitmentHash.inputs[6] <== ships[2][0];  // cruiser x
    boardCommitmentHash.inputs[7] <== ships[2][1];  // cruiser y
    boardCommitmentHash.inputs[8] <== ships[2][2];  // cruiser orientation
    boardCommitmentHash.inputs[9] <== ships[3][0];  // submarine x
    boardCommitmentHash.inputs[10] <== ships[3][1]; // submarine y
    boardCommitmentHash.inputs[11] <== ships[3][2]; // submarine orientation
    boardCommitmentHash.inputs[12] <== ships[4][0]; // destroyer x
    boardCommitmentHash.inputs[13] <== ships[4][1]; // destroyer y
    boardCommitmentHash.inputs[14] <== ships[4][2]; // destroyer orientation
    boardCommitmentHash.inputs[15] <== salt;

    component boardCommitmentCheck = IsEqual();
    boardCommitmentCheck.in[0] <== boardCommitmentHash.out;
    boardCommitmentCheck.in[1] <== boardCommitment;

    // Step 2: Verify previous state commitment
    component prevCommitmentHash = Poseidon(6);
    for (var i = 0; i < 5; i++) {
        prevCommitmentHash.inputs[i] <== previousHitCounts[i];
    }
    prevCommitmentHash.inputs[5] <== salt;

    component prevCommitmentCheck = IsEqual();
    prevCommitmentCheck.in[0] <== prevCommitmentHash.out;
    prevCommitmentCheck.in[1] <== previousCommitment;

    // Step 3: Generate ship coordinates
    component shipGenerators[5];
    shipGenerators[0] = GenerateShipCoords(5);  // Carrier
    shipGenerators[1] = GenerateShipCoords(4);  // Battleship
    shipGenerators[2] = GenerateShipCoords(3);  // Cruiser
    shipGenerators[3] = GenerateShipCoords(3);  // Submarine
    shipGenerators[4] = GenerateShipCoords(2);  // Destroyer

    for (var i = 0; i < 5; i++) {
        shipGenerators[i].x <== ships[i][0];
        shipGenerators[i].y <== ships[i][1];
        shipGenerators[i].orientation <== ships[i][2];
    }

    // Step 4: Check which ship (if any) is hit
    component hitCheckers[5];
    signal shipHitFlags[5];
    signal hitShipId;
    signal anyHit;

    hitCheckers[0] = CoordinateHitsShip(5);
    hitCheckers[1] = CoordinateHitsShip(4);
    hitCheckers[2] = CoordinateHitsShip(3);
    hitCheckers[3] = CoordinateHitsShip(3);
    hitCheckers[4] = CoordinateHitsShip(2);

    var hitSum = 0;
    var hitIdSum = 0;

    for (var i = 0; i < 5; i++) {
        hitCheckers[i].targetX <== targetX;
        hitCheckers[i].targetY <== targetY;
        
        // Connect ship coordinates
        for (var j = 0; j < SHIP_LENGTHS[i]; j++) {
            hitCheckers[i].shipCoords[j][0] <== shipGenerators[i].coords[j][0];
            hitCheckers[i].shipCoords[j][1] <== shipGenerators[i].coords[j][1];
        }
        
        shipHitFlags[i] <== hitCheckers[i].isHit;
        hitSum += shipHitFlags[i];
        hitIdSum += shipHitFlags[i] * i;
    }

    anyHit <== hitSum;
    hitShipId <== hitIdSum;  // Which ship was hit (0-4)

    // Step 5: Calculate new hit counts
    signal newHitCounts[5];
    for (var i = 0; i < 5; i++) {
        newHitCounts[i] <== previousHitCounts[i] + shipHitFlags[i];
    }

    // Step 6: Check if any ship is sunk and validate ship states
    component sunkCheckers[5];
    component alreadySunkCheckers[5];
    signal shipSunkFlags[5];
    signal shipAlreadySunkFlags[5];
    signal anySunk;
    signal sunkShipId;

    var sunkSum = 0;
    var sunkIdSum = 0;

    for (var i = 0; i < 5; i++) {
        // Check if ship is newly sunk (hit count equals ship length)
        sunkCheckers[i] = IsEqual();
        sunkCheckers[i].in[0] <== newHitCounts[i];
        sunkCheckers[i].in[1] <== SHIP_LENGTHS[i];
        shipSunkFlags[i] <== sunkCheckers[i].out;
        
        // Check if ship was already sunk before this shot
        alreadySunkCheckers[i] = IsEqual();
        alreadySunkCheckers[i].in[0] <== previousHitCounts[i];
        alreadySunkCheckers[i].in[1] <== SHIP_LENGTHS[i];
        shipAlreadySunkFlags[i] <== alreadySunkCheckers[i].out;
    }

    // Step 5.1: Calculate remaining ships (ships not yet sunk)
    var remainingShipsCount = 0;
    for (var i = 0; i < 5; i++) {
        // Ship is alive if it's not sunk (hit count < ship length)
        remainingShipsCount += (1 - shipSunkFlags[i]);
    }
    remainingShips <== remainingShipsCount;

    // Step 5.2: Validate elimination logic - newly sunk ships must be hit by this shot
    signal newlySunkValidationResults[5];
    signal wasNewlySunk[5];

    // Declare AND gates outside loop
    component newlySunkGate[5];
    for (var i = 0; i < 5; i++) {
        newlySunkGate[i] = AND();
    }

    var newlySunkValidationSum = 0;
    for (var i = 0; i < 5; i++) {
        // Check if ship is newly sunk: sunk now AND not sunk before
        // Use AND gate instead of subtraction to avoid field underflow
        newlySunkGate[i].a <== shipSunkFlags[i];
        newlySunkGate[i].b <== 1 - shipAlreadySunkFlags[i];  // NOT already sunk
        wasNewlySunk[i] <== newlySunkGate[i].out;
        
        // If wasNewlySunk=1, then shipHitFlags[i] must be 1
        // If wasNewlySunk=0, then shipHitFlags[i] can be anything
        // wasNewlySunk * (1 - shipHitFlags[i]) should always be 0
        newlySunkValidationResults[i] <== wasNewlySunk[i] * (1 - shipHitFlags[i]);
        newlySunkValidationSum += newlySunkValidationResults[i];
        
        // Use newly sunk ships for anySunk calculation
        sunkSum += wasNewlySunk[i];
        sunkIdSum += wasNewlySunk[i] * i;
    }

    // Assign computed values after loop completion
    anySunk <== sunkSum;
    sunkShipId <== sunkIdSum;

    // All newly sunk validation results should be 0
    component newlySunkValidationCheck = IsEqual();
    newlySunkValidationCheck.in[0] <== newlySunkValidationSum;
    newlySunkValidationCheck.in[1] <== 0;

    // Step 5.3: Remaining ships calculation (now an output, no validation needed)

    // Step 6: Verify claimed result matches actual result
    signal expectedResult;

    // For result calculation: Priority is Sunk > Hit > Miss
    // If any ship was newly sunk, result = 2
    // Otherwise if any ship was hit, result = 1
    // Otherwise result = 0

    // Constrain to binary values using GreaterThan
    component hasHit = GreaterThan(4); // Max 15 ships could be hit theoretically
    hasHit.in[0] <== anyHit;
    hasHit.in[1] <== 0;
    signal isHit <== hasHit.out;

    component hasSunk = GreaterThan(4); // Max 15 ships could be sunk theoretically  
    hasSunk.in[0] <== anySunk;
    hasSunk.in[1] <== 0;
    signal isSunk <== hasSunk.out;

    // Use conditional logic: result = isSunk * 2 + (1 - isSunk) * isHit
    expectedResult <== isSunk * 2 + (1 - isSunk) * isHit;

    component resultCheck = IsEqual();
    resultCheck.in[0] <== claimedResult;
    resultCheck.in[1] <== expectedResult;

    // Step 7: Verify claimed ship ID (only matters if sunk)
    signal expectedShipId;
    component shipIdSelector = Mux1();
    shipIdSelector.c[0] <== 255;  // Not sunk
    shipIdSelector.c[1] <== sunkShipId;  // Sunk ship ID
    shipIdSelector.s <== anySunk;
    expectedShipId <== shipIdSelector.out;

    component shipIdCheck = IsEqual();
    shipIdCheck.in[0] <== claimedShipId;
    shipIdCheck.in[1] <== expectedShipId;

    // Step 8: Generate new state commitment (now an output)
    component newCommitmentHash = Poseidon(6);
    for (var i = 0; i < 5; i++) {
        newCommitmentHash.inputs[i] <== newHitCounts[i];
    }
    newCommitmentHash.inputs[5] <== salt;

    // Assign computed commitment to output
    newCommitment <== newCommitmentHash.out;

    // Step 9: Assert all validations individually
    boardCommitmentCheck.out === 1;
    prevCommitmentCheck.out === 1;
    resultCheck.out === 1;
    shipIdCheck.out === 1;
    newlySunkValidationCheck.out === 1;
}

// Instantiate the main component with public inputs specified  
component main {public [previousCommitment, targetX, targetY, claimedResult, claimedShipId, boardCommitment]} = BattleshipShot();
