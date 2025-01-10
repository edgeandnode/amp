import { Schema } from "effect"

export type DatasetConfig = {
    name: string;
    version: string;
    repository?: string;

    // Optional, defaults to `Dataset.md` if not provided
    readme?: string;

    dependencies: Record<string, Dependency>;
    tables: Record<string, Query>;
    stream_handlers: Record<string, StreamHandler>;
}

type Dependency = {
    owner: string,
    name: string,
    version: string,
};

type Query = {
    sql: string,
}

type StreamDefinition = {
    stream: unknown,
    partitionBy: string,
};

type StreamHandler = {
    stream: StreamDefinition;
    handler: string; // Assuming ctx.handler_from_path returns a string
};

type Context = {};


export function defineDataset(fn: (ctx: Context) => DatasetConfig): DatasetConfig {
    return fn({})
}

