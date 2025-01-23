import { DatasetDefinition } from '../definition'
import * as fs from 'fs'
import * as path from 'path'
import { DatasetManifest, Table } from '../manifest'
import { RegistryService } from '../registry_service'

// Returns the path to the manifest.json file
export async function build(config: DatasetDefinition, out_dir: string): Promise<string> {
    console.log("building manifest")

    const manifest = await build_manifest(config)

    // Create the output directory if it doesn't exist
    if (!fs.existsSync(out_dir)) {
        fs.mkdirSync(out_dir)
    }

    // Output formatted JSON
    const json = JSON.stringify(manifest, null, 4)
    const manifest_path = out_dir + '/manifest.json'
    fs.writeFileSync(manifest_path, json)

    return manifest_path
}

async function build_manifest(config: DatasetDefinition): Promise<DatasetManifest> {
    const registry_service = new RegistryService()

    // build tables
    const tables: Record<string, Table> = {}
    for (const [name, query] of Object.entries(config.tables)) {
        tables[name] = {
            input: {
                sql: query.sql,
            },
            schema: await registry_service.output_schema(query.sql)
        }
    }

    return {
        kind: "manifest",
        name: config.name,
        version: config.version,
        dependencies: config.dependencies,
        tables,
    }
}