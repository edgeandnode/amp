import * as fs from 'fs'
import * as path from 'path'
import { DatasetManifest } from "../manifest";

export async function deploy_local(manifest_path: string, nozzle_url: string) {
    // `manifest_path` validation checks
    if (!fs.existsSync(manifest_path)) {
        throw new Error(`Path does not exist: ${manifest_path}`)
    }
    if (fs.statSync(manifest_path).isDirectory()) {
        throw new Error(
            `Expected a manifest file but got a directory: ${manifest_path}\n` +
            `Did you mean to specify a file like ${path.join(manifest_path, 'manifest.json')}?`
        )
    }

    const manifest: DatasetManifest = JSON.parse(fs.readFileSync(manifest_path, 'utf8'))

    try {
        await deploy_to_nozzle(manifest, nozzle_url)
        console.log('Dataset deployed to nozzle')
    } catch (error) {
        console.error('Error deploying to nozzle:', error)
        throw error
    }
}

async function deploy_to_nozzle(manifest: DatasetManifest, nozzle_url: string) {
    const response = await fetch(`${nozzle_url}/deploy`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ dataset_name: manifest.name, manifest: JSON.stringify(manifest) }),
    })
    if (!response.ok) {
        throw new Error(`Failed to deploy to nozzle: ${response.statusText}`)
    }
}
