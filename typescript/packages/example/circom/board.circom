pragma circom 2.1.0;

include "../node_modules/circomlib/circuits/poseidon.circom";
include "./ship.circom";

/**
 * @title Battleship Board Validation Circuit
 * @dev Main circuit for validating complete Battleship board setup
 * @notice Proves that a board commitment represents a valid ship arrangement
 */
template BattleshipBoard() {
    // Private inputs: ship positions and salt
    signal input carrier[3];     // [x, y, orientation] - Length 5
    signal input battleship[3];  // [x, y, orientation] - Length 4  
    signal input cruiser[3];     // [x, y, orientation] - Length 3
    signal input submarine[3];   // [x, y, orientation] - Length 3
    signal input destroyer[3];   // [x, y, orientation] - Length 2
    signal input salt;   // Random salt for commitment uniqueness

    // Public outputs: board commitment and initial state commitment
    signal output commitment;
    signal output initialStateCommitment;

    // Validate each ship individually first (early exit on placement errors)
    component validateCarrier = ValidateShip(5);
    validateCarrier.x <== carrier[0];
    validateCarrier.y <== carrier[1];
    validateCarrier.orientation <== carrier[2];

    component validateBattleship = ValidateShip(4);
    validateBattleship.x <== battleship[0];
    validateBattleship.y <== battleship[1];
    validateBattleship.orientation <== battleship[2];

    component validateCruiser = ValidateShip(3);
    validateCruiser.x <== cruiser[0];
    validateCruiser.y <== cruiser[1];
    validateCruiser.orientation <== cruiser[2];

    component validateSubmarine = ValidateShip(3);
    validateSubmarine.x <== submarine[0];
    validateSubmarine.y <== submarine[1];
    validateSubmarine.orientation <== submarine[2];

    component validateDestroyer = ValidateShip(2);
    validateDestroyer.x <== destroyer[0];
    validateDestroyer.y <== destroyer[1];
    validateDestroyer.orientation <== destroyer[2];

    // Check for overlaps by verifying all coordinates are unique (much more efficient!)
    // Collect all ship coordinates into a single array
    signal allCoords[17][2]; // Total: 5+4+3+3+2 = 17 coordinates

    // Carrier coordinates (5)
    for (var i = 0; i < 5; i++) {
        allCoords[i][0] <== validateCarrier.coords[i][0];
        allCoords[i][1] <== validateCarrier.coords[i][1];
    }

    // Battleship coordinates (4)
    for (var i = 0; i < 4; i++) {
        allCoords[5 + i][0] <== validateBattleship.coords[i][0];
        allCoords[5 + i][1] <== validateBattleship.coords[i][1];
    }

    // Cruiser coordinates (3)
    for (var i = 0; i < 3; i++) {
        allCoords[9 + i][0] <== validateCruiser.coords[i][0];
        allCoords[9 + i][1] <== validateCruiser.coords[i][1];
    }

    // Submarine coordinates (3)
    for (var i = 0; i < 3; i++) {
        allCoords[12 + i][0] <== validateSubmarine.coords[i][0];
        allCoords[12 + i][1] <== validateSubmarine.coords[i][1];
    }

    // Destroyer coordinates (2)
    for (var i = 0; i < 2; i++) {
        allCoords[15 + i][0] <== validateDestroyer.coords[i][0];
        allCoords[15 + i][1] <== validateDestroyer.coords[i][1];
    }

    // Encode coordinates as single numbers for more efficient comparison
    signal encoded[17];
    for (var i = 0; i < 17; i++) {
        encoded[i] <== allCoords[i][0] * 10 + allCoords[i][1]; // x*10 + y (0-99)
    }

    // Optimized O(n²) pairwise uniqueness verification with coordinate encoding
    // 17 coordinates = 17×16/2 = 136 pairwise comparisons (much better than 1,003!)
    component uniquenessCheck[136];
    var checkIndex = 0;

    for (var i = 0; i < 17; i++) {
        for (var j = i + 1; j < 17; j++) {
            uniquenessCheck[checkIndex] = IsEqual();
            uniquenessCheck[checkIndex].in[0] <== encoded[i];
            uniquenessCheck[checkIndex].in[1] <== encoded[j];
            // Assert coordinates are different (not equal)
            uniquenessCheck[checkIndex].out === 0;
            checkIndex++;
        }
    }

    // Generate Poseidon hash commitment
    // We'll use Poseidon with 16 inputs: 5 ships * 3 coords each + salt
    component hasher = Poseidon(16);

    // Input all ship data and salt into hasher
    hasher.inputs[0] <== carrier[0];
    hasher.inputs[1] <== carrier[1];
    hasher.inputs[2] <== carrier[2];
    hasher.inputs[3] <== battleship[0];
    hasher.inputs[4] <== battleship[1];
    hasher.inputs[5] <== battleship[2];
    hasher.inputs[6] <== cruiser[0];
    hasher.inputs[7] <== cruiser[1];
    hasher.inputs[8] <== cruiser[2];
    hasher.inputs[9] <== submarine[0];
    hasher.inputs[10] <== submarine[1];
    hasher.inputs[11] <== submarine[2];
    hasher.inputs[12] <== destroyer[0];
    hasher.inputs[13] <== destroyer[1];
    hasher.inputs[14] <== destroyer[2];
    hasher.inputs[15] <== salt;

    // Output the board commitment
    commitment <== hasher.out;

    // Generate initial state commitment for shot circuit
    // Initial hit counts are always [0, 0, 0, 0, 0] with the same salt
    component initialStateHasher = Poseidon(6);
    initialStateHasher.inputs[0] <== 0;  // carrier hit count
    initialStateHasher.inputs[1] <== 0;  // battleship hit count
    initialStateHasher.inputs[2] <== 0;  // cruiser hit count
    initialStateHasher.inputs[3] <== 0;  // submarine hit count
    initialStateHasher.inputs[4] <== 0;  // destroyer hit count
    initialStateHasher.inputs[5] <== salt;

    // Output the initial state commitment
    initialStateCommitment <== initialStateHasher.out;
}

// Instantiate the main component
component main = BattleshipBoard();
