/**
 * DatasetManifest to Metadata Converter
 *
 * Converts DatasetManifest tables (from amp configs) into unified table metadata
 * format for SQL intellisense. Handles Arrow schema to SQL type conversion.
 *
 * @file manifestConverter.ts
 */

import type { DatasetManifest } from "@edgeandnode/amp/Model"

import { arrowTypeToSql } from "../../utils/arrowTypeToSql.ts"
import type { ManifestTableMetadata } from "./types.ts"

/**
 * Converts DatasetManifest tables to unified metadata format for intellisense.
 *
 * @param manifests - Array of DatasetManifest instances from amp config
 * @returns Array of ManifestTableMetadata for use in SQL providers
 *
 * @example
 * const metadata = convertManifestsToMetadata([manifest1, manifest2])
 * // Returns: [
 * //   { source: "example.blocks", network: "mainnet", columns: [...] },
 * //   { source: "example.transactions", network: "mainnet", columns: [...] }
 * // ]
 */
export function convertManifestsToMetadata(manifests: ReadonlyArray<DatasetManifest>): Array<ManifestTableMetadata> {
  const result: Array<ManifestTableMetadata> = []

  for (const manifest of manifests) {
    // Extract dataset name and network
    const datasetName = manifest.name
    const network = manifest.network

    // Convert each table in the manifest
    for (const [tableName, table] of Object.entries(manifest.tables)) {
      const fullyQualifiedName = `${datasetName}.${tableName}`

      // Convert Arrow schema fields to column metadata
      const columns = table.schema.arrow.fields.map((field: { name: string; type: any; nullable: boolean }) => ({
        name: field.name,
        datatype: arrowTypeToSql(field.type),
        nullable: field.nullable,
      }))

      result.push({
        source: fullyQualifiedName,
        network,
        columns,
      })
    }
  }

  return result
}

/**
 * Converts a single DatasetManifest to metadata format.
 *
 * @param manifest - Single DatasetManifest instance
 * @returns Array of ManifestTableMetadata (one per table in manifest)
 */
export function convertManifestToMetadata(manifest: DatasetManifest): Array<ManifestTableMetadata> {
  return convertManifestsToMetadata([manifest])
}

/**
 * Merges manifest-based metadata with raw dataset sources into a unified format.
 * This allows intellisense to work with both raw sources (like anvil.blocks) and
 * materialized datasets (like example.blocks).
 *
 * @param rawSources - Raw DatasetSource[] from API
 * @param manifestMetadata - Converted manifest metadata
 * @returns Combined array in unified format
 */
export function mergeMetadataSources(
  rawSources: ReadonlyArray<{
    readonly source: string
    readonly metadata_columns: ReadonlyArray<{ readonly name: string; readonly datatype: string }>
  }>,
  manifestMetadata: ReadonlyArray<ManifestTableMetadata>,
): Array<ManifestTableMetadata> {
  // Convert raw sources to unified format
  const convertedRaw: Array<ManifestTableMetadata> = rawSources.map((source) => ({
    source: source.source,
    network: "unknown", // Raw sources may not have network info
    columns: source.metadata_columns.map((col) => ({
      name: col.name,
      datatype: col.datatype,
      nullable: true, // Raw sources don't provide nullable info
    })),
  }))

  // Merge both arrays
  return [...convertedRaw, ...manifestMetadata]
}
