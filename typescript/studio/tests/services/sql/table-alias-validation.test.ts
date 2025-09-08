/**
 * Test suite for SQL table alias validation
 * 
 * These tests verify that the SQL validator correctly handles table aliases
 * in qualified column references like tbl_a.column_name
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { SqlValidator } from '../../../src/services/sql/sqlValidator'
import { mockMetadata } from './fixtures/mockMetadata'
import { DEFAULT_COMPLETION_CONFIG } from '../../../src/services/sql/types'

describe('SqlValidator - Table Alias Validation', () => {
  let validator: SqlValidator

  beforeEach(() => {
    validator = new SqlValidator(mockMetadata, [], {
      ...DEFAULT_COMPLETION_CONFIG,
      enableDebugLogging: true,
      enableSqlValidation: true,
      validationLevel: 'full'
    })
  })

  afterEach(() => {
    validator.dispose()
  })

  describe('Simple table aliases', () => {
    test('should validate qualified columns with AS alias', () => {
      const query = 'SELECT tbl_a.hash FROM anvil.transactions AS tbl_a'
      const errors = validator.validateQuery(query)
      
      console.log('Errors for AS alias:', errors)
      expect(errors).toHaveLength(0) // hash is a valid column in anvil.transactions
    })

    test('should validate qualified columns with implicit alias', () => {
      const query = 'SELECT tbl_a.hash FROM anvil.transactions tbl_a' 
      const errors = validator.validateQuery(query)
      
      console.log('Errors for implicit alias:', errors)
      expect(errors).toHaveLength(0) // hash is a valid column in anvil.transactions
    })

    test('should report error for invalid column with alias', () => {
      const query = 'SELECT tbl_a.invalid_column FROM anvil.transactions AS tbl_a'
      const errors = validator.validateQuery(query)
      
      console.log('Errors for invalid column:', errors)
      expect(errors).toHaveLength(1)
      expect(errors[0].message).toContain('invalid_column')
      expect(errors[0].message).toContain('not found')
    })

    test('should report error for undefined alias', () => {
      const query = 'SELECT unknown_alias.hash FROM anvil.transactions AS tbl_a'
      const errors = validator.validateQuery(query)
      
      console.log('Errors for undefined alias:', errors) 
      expect(errors).toHaveLength(1)
      expect(errors[0].message).toContain('unknown_alias')
    })
  })

  describe('Complex JOIN queries with aliases', () => {
    test('should validate columns from different aliased tables', () => {
      const query = `
        SELECT tbl_a.hash, tbl_b.address 
        FROM anvil.transactions AS tbl_a 
        LEFT JOIN anvil.logs AS tbl_b ON tbl_a.hash = tbl_b.transaction_hash
      `
      const errors = validator.validateQuery(query)
      
      console.log('Errors for JOIN query:', errors)
      expect(errors).toHaveLength(0)
    })

    test('should validate complex JOIN with mixed qualified/unqualified columns', () => {
      const query = `
        SELECT 
          tbl_a.hash,
          address,
          tbl_b.transaction_hash
        FROM anvil.transactions AS tbl_a 
        LEFT JOIN anvil.logs AS tbl_b ON tbl_a.hash = tbl_b.transaction_hash
      `
      const errors = validator.validateQuery(query)
      
      console.log('Errors for mixed column query:', errors)
      // address should be valid (exists in anvil.logs)
      // tbl_a.hash should be valid 
      // tbl_b.transaction_hash should be valid
      expect(errors).toHaveLength(0)
    })
  })

  describe('Alias parsing edge cases', () => {
    test('should handle aliases with underscores', () => {
      const query = 'SELECT my_alias.hash FROM anvil.transactions AS my_alias'
      const errors = validator.validateQuery(query)
      
      expect(errors).toHaveLength(0)
    })

    test('should handle aliases with numbers', () => {
      const query = 'SELECT tbl1.hash FROM anvil.transactions AS tbl1'
      const errors = validator.validateQuery(query)
      
      expect(errors).toHaveLength(0)
    })

    test('should be case insensitive for AS keyword', () => {
      const query = 'SELECT tbl_a.hash FROM anvil.transactions as tbl_a'
      const errors = validator.validateQuery(query)
      
      expect(errors).toHaveLength(0)
    })

    test('should handle multiple whitespace around AS', () => {
      const query = 'SELECT tbl_a.hash FROM anvil.transactions   AS   tbl_a'  
      const errors = validator.validateQuery(query)
      
      expect(errors).toHaveLength(0)
    })
  })

  describe('Real world scenarios', () => {
    test('should validate the original user query', () => {
      // Based on user's original issue  
      const query = `
        SELECT address, tbl_a.block_number, transaction_hash, transaction_index 
        FROM anvil.transactions as tbl_a 
        LEFT JOIN anvil.logs as tbl_b  
        ON tbl_a.hash = tbl_b.transaction_hash
      `
      const errors = validator.validateQuery(query)
      
      console.log('Errors for user query:', errors)
      
      // Expected errors (based on actual column names):
      // - address: invalid (not qualified, exists only in anvil.logs)  
      // - transaction_hash: invalid (not in metadata)
      // - transaction_index: invalid (not in metadata)
      // - tbl_a.block_number: should be valid (anvil.transactions has block_number)
      
      // At minimum, tbl_a.block_number should NOT have an error
      const aliasErrors = errors.filter(e => e.message.includes('tbl_a'))
      expect(aliasErrors).toHaveLength(0) // No errors for properly aliased columns
    })
  })
})