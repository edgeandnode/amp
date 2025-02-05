export type DatasetManifest = {
    kind: "manifest";
    name: string;
    version: string;
    repository?: string;

    // Optional, defaults to `Dataset.md` if not provided
    readme?: string;

    dependencies: Record<string, Dependency>;
    tables: Record<string, Table>;
}

export type Dependency = {
    owner: string,
    name: string,
    version: string,
};

export type Table = {
    input: TableInput,
    schema: TableSchema,
}

export type TableInput = View;

export type View = {
    sql: string,
}

export type TableSchema = {
    arrow: ArrowSchema,
}

export type ArrowSchema = {
    fields: Field[],
}

export type Field = {
    name: string,
    type: string,
    nullable: boolean,
}
