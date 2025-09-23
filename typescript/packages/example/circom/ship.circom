pragma circom 2.1.0;

include "./utils.circom";
include "../node_modules/circomlib/circuits/comparators.circom";

/**
 * @title Ship Validation Circuit
 * @dev Validates individual ship placement and constraints
 */

/**
 * @dev Validate a single ship placement
 * @param shipLength The expected length of the ship
 */
template ValidateShip(shipLength) {
    signal input x;           // Starting x coordinate
    signal input y;           // Starting y coordinate
    signal input orientation; // 0 = horizontal, 1 = vertical
    signal output coords[shipLength][2]; // Output ship coordinates for overlap checking

    // Generate ship coordinates
    component genCoords = GenerateShipCoords(shipLength);
    genCoords.x <== x;
    genCoords.y <== y;
    genCoords.orientation <== orientation;

    // Output coordinates for use in overlap checking
    for (var i = 0; i < shipLength; i++) {
        coords[i][0] <== genCoords.coords[i][0];
        coords[i][1] <== genCoords.coords[i][1];
    }

    // Validate orientation - ValidOrientation now asserts directly
    component validOrient = ValidOrientation();
    validOrient.orientation <== orientation;

    // Validate all coordinates are in bounds - ShipInBounds now asserts directly
    component inBounds = ShipInBounds(shipLength);
    for (var i = 0; i < shipLength; i++) {
        inBounds.coords[i][0] <== coords[i][0];
        inBounds.coords[i][1] <== coords[i][1];
    }
}
