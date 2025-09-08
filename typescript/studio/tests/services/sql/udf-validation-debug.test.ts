/**
 * Debug UDF validation in Studio query editor
 * Test the specific query that's showing error markers for evm_topic and evm_decode_log
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { SqlValidator } from '../../../src/services/sql/sqlValidator'
import { mockMetadata } from './fixtures/mockMetadata'
import { mockUDFs } from './fixtures/mockUDFs'
import type { DatasetMetadata, CompletionConfig } from '../../../src/services/sql/types'

// Convert metadata to the format expected by SqlValidator  
const convertMetadata = (metadata: typeof mockMetadata): DatasetMetadata[] => {
  return metadata.map(dataset => ({
    dataset_name: dataset.source.split('.')[0],
    table_name: dataset.source.split('.')[1] || dataset.destination,
    column_names: dataset.metadata_columns.map(col => col.name),
    column_types: dataset.metadata_columns.map(col => col.datatype),
    dataset_size: '100MB',
    record_count: 50000,
    column_stats: {}
  }))
}

describe('UDF Validation Debug', () => {
  let validator: SqlValidator
  let config: CompletionConfig

  beforeEach(() => {
    config = {
      minPrefixLength: 0,
      maxSuggestions: 50,
      enableSnippets: true,
      enableContextFiltering: true,
      enableAliasResolution: true,
      contextCacheTTL: 30 * 1000,
      enableDebugLogging: true, // Enable debug logging
      enableSqlValidation: true,
      validationLevel: 'full',
      enablePartialValidation: true
    }

    const testDatasets = convertMetadata(mockMetadata)
    validator = new SqlValidator(testDatasets, mockUDFs, config)
    
    console.log('📋 Available UDFs:', mockUDFs.map(u => u.name))
    console.log('📋 Available tables:', testDatasets.map(d => `${d.dataset_name}.${d.table_name}`))
  })

  afterEach(() => {
    validator.dispose()
  })

  test('should validate UDFs in SELECT and WHERE clauses with corrected column names', () => {
    // Use actual column names from anvil.logs metadata
    const query = `SELECT transaction_hash, data, evm_decode_log(topics, data, 'Count') FROM anvil.logs WHERE address = evm_topic('Count')`
    
    console.log('\n🧪 Testing corrected query:')
    console.log(query)
    
    const errors = validator.validateQuery(query)
    
    console.log(`\n📊 Found ${errors.length} validation errors:`)
    if (errors.length > 0) {
      errors.forEach((error, i) => {
        console.log(`\nError ${i + 1}:`)
        console.log(`  Code: ${error.code}`)
        console.log(`  Message: ${error.message}`)
        console.log(`  Position: Line ${error.startLineNumber}, Col ${error.startColumn}-${error.endColumn}`)
      })
    } else {
      console.log('✅ No validation errors - UDFs and columns are properly validated!')
    }
    
    expect(errors).toHaveLength(0) // Should have no errors with correct column names
  })

  test('should recognize evm_topic as valid UDF', () => {
    const query = `SELECT evm_topic('Count') FROM anvil.logs`
    console.log('\n🧪 Testing simple evm_topic query:', query)
    
    const errors = validator.validateQuery(query)
    console.log(`📊 Found ${errors.length} errors`)
    
    errors.forEach((error, i) => {
      console.log(`Error ${i + 1}: ${error.message}`)
    })
  })

  test('should recognize evm_decode_log as valid UDF', () => {
    const query = `SELECT evm_decode_log(topic1, topic2, topic3, data, 'Count') FROM anvil.logs`
    console.log('\n🧪 Testing simple evm_decode_log query:', query)
    
    const errors = validator.validateQuery(query)
    console.log(`📊 Found ${errors.length} errors`)
    
    errors.forEach((error, i) => {
      console.log(`Error ${i + 1}: ${error.message}`)
    })
  })

  test('should list available UDFs', () => {
    console.log('\n📋 All available UDFs:')
    mockUDFs.forEach((udf, i) => {
      console.log(`${i + 1}. ${udf.name}`)
      console.log(`   SQL: ${udf.sql}`)
      console.log(`   Parameters: [${udf.parameters.join(', ')}]`)
    })
  })

  test('should validate wildcard with table alias', () => {
    const query = `SELECT l.*, t.hash FROM anvil.logs as l JOIN anvil.transactions as t ON l.transaction_hash = t.hash`
    
    console.log('\n🧪 Testing corrected wildcard alias query:')
    console.log(query)
    
    const errors = validator.validateQuery(query)
    
    console.log(`\n📊 Found ${errors.length} validation errors:`)
    if (errors.length > 0) {
      errors.forEach((error, i) => {
        console.log(`\nError ${i + 1}:`)
        console.log(`  Code: ${error.code}`)
        console.log(`  Message: ${error.message}`)
        console.log(`  Position: Line ${error.startLineNumber}, Col ${error.startColumn}-${error.endColumn}`)
      })
    } else {
      console.log('✅ No validation errors - wildcard aliases work perfectly!')
    }
    
    expect(errors).toHaveLength(0) // Should have no errors with correct column names and wildcard
  })
})