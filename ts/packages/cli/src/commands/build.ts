import { DatasetConfig } from '../definition'
import * as fs from 'fs'

// Returns the path to the manifest.json file
export function build(config: DatasetConfig, output_path: string): string {
    const json = JSON.stringify(config)
    const manifest_path = output_path + '/manifest.json'
    fs.writeFileSync(manifest_path, json)
    return manifest_path
}
