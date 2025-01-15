import { DatasetConfig } from '../definition'
import * as fs from 'fs'
import * as path from 'path'

// Returns the path to the manifest.json file
export function build(config: DatasetConfig): string {
    // Create the output directory if it doesn't exist
    const dist_path = path.resolve(__dirname, "nozzle_dist")
    if (!fs.existsSync(dist_path)) {
        fs.mkdirSync(dist_path)
    }
    const json = JSON.stringify(config)
    const manifest_path = dist_path + '/manifest.json'
    fs.writeFileSync(manifest_path, json)
    return manifest_path
}
