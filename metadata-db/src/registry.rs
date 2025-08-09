use object_store::path::Path;
use sqlx::{Executor, Postgres};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Registry {
    pub owner: String,
    pub dataset: String,
    pub version: String,
    pub manifest_path: Path,
}

#[tracing::instrument(skip(exe), err)]
pub async fn insert_dataset_to_registry<'c, E>(
    exe: E,
    registry_info: Registry,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let sql = "
    INSERT INTO registry (dataset, version, manifest, owner)
    VALUES ($1, $2, $3, $4)
    ";
    sqlx::query(sql)
        .bind(registry_info.dataset)
        .bind(registry_info.version)
        .bind(registry_info.manifest_path.to_string())
        .bind(registry_info.owner)
        .execute(exe)
        .await?;

    Ok(())
}
