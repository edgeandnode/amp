import { TableSchema } from './manifest'

export class RegistryService {
    private url: string
    constructor() {
        // Get the url from the env
        const registry_service_url = process.env.NOZZLE_REGISTRY_SERVICE_URL
        if (!registry_service_url) {
            throw new Error('NOZZLE_REGISTRY_SERVICE_URL environment variable is not set')
        }
        this.url = registry_service_url
    }

    async output_schema(sql_query: string): Promise<TableSchema> {
        const response = await fetch(`${this.url}/output_schema`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ sql_query }),
        })
        const schema = (await response.json()).schema as TableSchema
        return schema
    }
}
