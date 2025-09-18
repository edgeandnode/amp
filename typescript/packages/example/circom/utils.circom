pragma circom 2.1.0;

include "../node_modules/circomlib/circuits/comparators.circom";

/**
 * @title Battleship Utility Templates
 * @dev Helper templates for Battleship zkSNARK circuits
 */

/**
 * @dev Check if a coordinate is within bounds (0-9)
 */
template InBounds() {
    signal input coord;

    component gte = GreaterEqThan(4); // 4 bits can represent 0-15
    component lte = LessEqThan(4);

    gte.in[0] <== coord;
    gte.in[1] <== 0;

    lte.in[0] <== coord;
    lte.in[1] <== 9;

    // Assert coordinate is within bounds
    gte.out === 1;
    lte.out === 1;
}

/**
 * @dev Check if two coordinates are equal
 */
template CoordEqual() {
    signal input coord1[2]; // [x, y]
    signal input coord2[2]; // [x, y]
    signal output equal;

    component eqX = IsEqual();
    component eqY = IsEqual();

    eqX.in[0] <== coord1[0];
    eqX.in[1] <== coord2[0];

    eqY.in[0] <== coord1[1];
    eqY.in[1] <== coord2[1];

    equal <== eqX.out * eqY.out;
}

/**
 * @dev Generate all coordinates for a ship given its position and orientation
 * @param shipLength The length of the ship (2-5)
 */
template GenerateShipCoords(shipLength) {
    signal input x;           // Starting x coordinate
    signal input y;           // Starting y coordinate  
    signal input orientation; // 0 = horizontal, 1 = vertical
    signal output coords[shipLength][2]; // All ship coordinates

    // Generate coordinates based on orientation
    for (var i = 0; i < shipLength; i++) {
        // If horizontal (orientation = 0): x + i, y
        // If vertical (orientation = 1): x, y + i
        coords[i][0] <== x + i * (1 - orientation);
        coords[i][1] <== y + i * orientation;
    }
}

/**
 * @dev Check if any coordinate in a ship is out of bounds
 */
template ShipInBounds(shipLength) {
    signal input coords[shipLength][2];

    component inBounds[shipLength * 2]; // Check both x and y for each coordinate

    // Check each coordinate - InBounds now asserts directly
    for (var i = 0; i < shipLength; i++) {
        inBounds[i * 2] = InBounds();
        inBounds[i * 2 + 1] = InBounds();
        
        inBounds[i * 2].coord <== coords[i][0];
        inBounds[i * 2 + 1].coord <== coords[i][1];
        // No need to collect outputs - InBounds asserts directly
    }
}

/**
 * @dev Validate orientation is either 0 (horizontal) or 1 (vertical)
 */
template ValidOrientation() {
    signal input orientation;

    // orientation * (orientation - 1) should equal 0
    // This is true only when orientation is 0 or 1
    signal validCheck <== orientation * (orientation - 1);
    validCheck === 0;
}
