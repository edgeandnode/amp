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

    // Assume a local IPFS node
    const ipfsApiUrl = 'http://127.0.0.1:5001/api/v0'

    let cid: string;
    try {
        cid = await upload_to_ipfs(manifest, ipfsApiUrl)
        console.log(`Manifest uploaded to IPFS with CID: ${cid}`)
    } catch (error) {
        console.error('Error uploading to IPFS:', error)
        throw error
    }

    try {
        await deploy_to_nozzle(cid, nozzle_url)
        console.log('Dataset deployed to nozzle')
    } catch (error) {
        console.error('Error deploying to nozzle:', error)
        throw error
    }
}

async function upload_to_ipfs(manifest: DatasetManifest, ipfs: string): Promise<string> {
    const manifestBuffer = Buffer.from(JSON.stringify(manifest))
    const formData = new FormData()
    formData.append('file', new Blob([manifestBuffer]))
    const response = await fetch(`${ipfs}/add`, {
        method: 'POST',
        body: formData,
    })

    if (!response.ok) {
        throw new Error(`Failed to upload to IPFS: ${response.statusText}`)
    }

    const data = await response.json() as IpfsAddResponse
    return data.Hash
}

type IpfsAddResponse = {
    Name: string;
    Hash: string;
    Size: string;
}

async function deploy_to_nozzle(cid: string, nozzle_url: string) {
    const response = await fetch(`${nozzle_url}/deploy`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ cid }),
    })
    if (!response.ok) {
        throw new Error(`Failed to deploy to nozzle: ${response.statusText}`)
    }
}
