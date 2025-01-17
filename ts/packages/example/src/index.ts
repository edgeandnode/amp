import { build, deploy_local } from 'project-nozzle'
import dataset from './dataset'

const manifest_path = build(dataset)
deploy_local(manifest_path, 'http://localhost:1603')
