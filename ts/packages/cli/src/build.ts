import { DatasetConfig } from './definition'
import * as fs from 'fs'

export function build(config: DatasetConfig, output_path: string) {
    let json = JSON.stringify(config)
    fs.writeFileSync(output_path + '/manifest.json', json)
}
