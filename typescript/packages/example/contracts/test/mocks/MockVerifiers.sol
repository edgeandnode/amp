// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

/**
 * @title Mock Board Verifier
 * @dev Mock zkSNARK verifier for board commitments (always returns true for testing)
 */
contract MockBoardVerifier {
    bool public shouldReturnTrue = true;

    function verifyProof(uint256[2] memory, uint256[2][2] memory, uint256[2] memory, uint256[1] memory)
        external
        view
        returns (bool)
    {
        return shouldReturnTrue;
    }

    function setShouldReturnTrue(bool _shouldReturnTrue) external {
        shouldReturnTrue = _shouldReturnTrue;
    }
}

/**
 * @title Mock Shot Verifier
 * @dev Mock zkSNARK verifier for shot proofs (always returns true for testing)
 */
contract MockShotVerifier {
    bool public shouldReturnTrue = true;

    function verifyProof(uint256[2] memory, uint256[2][2] memory, uint256[2] memory, uint256[7] memory)
        external
        view
        returns (bool)
    {
        return shouldReturnTrue;
    }

    function setShouldReturnTrue(bool _shouldReturnTrue) external {
        shouldReturnTrue = _shouldReturnTrue;
    }
}
