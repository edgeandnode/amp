import { Schema } from "effect"

export type DatasetDefinition = {
    name: string;
    version: string;
    repository?: string;

    // Optional, defaults to `Dataset.md` if not provided
    readme?: string;

    dependencies: Record<string, Dependency>;
    tables: Record<string, Table>;
}

type Dependency = {
    owner: string,
    name: string,
    version: string,
};

type Table = View;

type View = {
    sql: string,
}

type Context = {};


export function defineDataset(fn: (ctx: Context) => DatasetDefinition): DatasetDefinition {
    return fn({})
}

