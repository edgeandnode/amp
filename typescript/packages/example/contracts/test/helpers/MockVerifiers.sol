// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

/**
 * @title Mock Board Verifier
 * @dev Mock zkSNARK verifier for board commitments (always returns true for testing)
 */
contract MockBoardVerifier {
    bool public shouldReturnTrue = true;

    function verifyProof(uint256[2] memory, uint256[2][2] memory, uint256[2] memory, uint256[2] memory)
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
 * @title Mock Impact Verifier
 * @dev Mock zkSNARK verifier for impact proofs (always returns true for testing)
 */
contract MockImpactVerifier {
    bool public shouldReturnTrue = true;

    function verifyProof(uint256[2] memory, uint256[2][2] memory, uint256[2] memory, uint256[8] memory)
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
