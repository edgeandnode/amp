/**
 * Debug test for ambiguous column table suggestions
 * Testing the specific user query to identify suggestion issues
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { SqlValidator } from '../../../src/services/sql/sqlValidator'
import { mockMetadata } from './fixtures/mockMetadata'
import { DEFAULT_COMPLETION_CONFIG } from '../../../src/services/sql/types'

describe('SqlValidator - Ambiguous Column Suggestions Debug', () => {
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

  test('should debug table suggestions for ambiguous columns', () => {
    const query = 'SELECT address, block_num, block_hash FROM anvil.transactions as t1 LEFT JOIN anvil.logs as t2 ON t1.address = t2.id'
    const errors = validator.validateQuery(query)
    
    console.log('=== Query Analysis ===')
    console.log('Query:', query)
    console.log('Number of errors:', errors.length)
    
    console.log('\n=== Available Metadata ===')
    mockMetadata.forEach(dataset => {
      console.log(`\n${dataset.source}:`)
      dataset.metadata_columns.forEach(col => {
        console.log(`  - ${col.name} (${col.datatype})`)
      })
    })
    
    console.log('\n=== Error Details ===')
    errors.forEach((error, index) => {
      console.log(`\nError ${index + 1}:`)
      console.log('  Message:', error.message)
      console.log('  Code:', error.code)
      console.log('  Severity:', error.severity)
      console.log('  Position:', `Line ${error.startLineNumber}, Col ${error.startColumn}-${error.endColumn}`)
      if (error.data) {
        console.log('  Data:', JSON.stringify(error.data, null, 4))
      }
    })
    
    // Check specifically for block_num and block_hash errors
    const blockNumError = errors.find(e => e.data?.columnName === 'block_num')
    const blockHashError = errors.find(e => e.data?.columnName === 'block_hash')
    
    if (blockNumError) {
      console.log('\n=== block_num Error Analysis ===')
      console.log('Error message:', blockNumError.message)
      console.log('Tables data:', blockNumError.data?.tables)
      console.log('Expected: Should suggest tables t1 and t2 since both have block_number column')
    }
    
    if (blockHashError) {
      console.log('\n=== block_hash Error Analysis ===')
      console.log('Error message:', blockHashError.message)
      console.log('Tables data:', blockHashError.data?.tables)
      console.log('Expected: Should suggest tables t1 and t2 since both might have block_hash-related columns')
    }
    
    // The test should pass - we're just debugging the current behavior
    expect(errors.length).toBeGreaterThan(0)
  })

  test('should check what columns actually exist in each table', () => {
    console.log('\n=== Detailed Column Analysis ===')
    
    // Check for block_number variations
    const transactionsTable = mockMetadata.find(m => m.source === 'anvil.transactions')
    const logsTable = mockMetadata.find(m => m.source === 'anvil.logs')
    
    console.log('\nanvil.transactions columns:')
    transactionsTable?.metadata_columns.forEach(col => {
      if (col.name.includes('block')) {
        console.log(`  - ${col.name} (${col.datatype}) ← BLOCK RELATED`)
      } else {
        console.log(`  - ${col.name} (${col.datatype})`)
      }
    })
    
    console.log('\nanvil.logs columns:')
    logsTable?.metadata_columns.forEach(col => {
      if (col.name.includes('block')) {
        console.log(`  - ${col.name} (${col.datatype}) ← BLOCK RELATED`)
      } else {
        console.log(`  - ${col.name} (${col.datatype})`)
      }
    })
    
    // Test specific columns
    const blockNumInTransactions = transactionsTable?.metadata_columns.some(col => col.name === 'block_num')
    const blockNumInLogs = logsTable?.metadata_columns.some(col => col.name === 'block_num')
    const blockNumberInTransactions = transactionsTable?.metadata_columns.some(col => col.name === 'block_number')
    const blockNumberInLogs = logsTable?.metadata_columns.some(col => col.name === 'block_number')
    
    console.log('\n=== Column Existence Check ===')
    console.log('block_num in anvil.transactions:', blockNumInTransactions)
    console.log('block_num in anvil.logs:', blockNumInLogs)
    console.log('block_number in anvil.transactions:', blockNumberInTransactions)
    console.log('block_number in anvil.logs:', blockNumberInLogs)
    
    expect(transactionsTable).toBeDefined()
    expect(logsTable).toBeDefined()
  })
})