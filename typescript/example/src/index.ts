import { build, deploy_local } from '@nozzle/cli'
import dataset from './dataset.js'
import * as path from 'path'

async function main() {
    const out_dir = path.resolve(__dirname, "nozzle_dist")
    const manifest_path = await build(dataset, out_dir)
    await deploy_local(manifest_path, 'http://localhost:1610')
}

main().catch(console.error)
