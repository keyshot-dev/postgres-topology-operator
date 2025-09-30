use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::types::{HasPostgresAdminConnection, PostgresAdminConnectionReference, PostgresRoleReference};


#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "postgres.digizuite.com",
    version = "v1alpha1",
    kind = "PostgresDatabase",
    plural = "postgresdatabases",
    derive = "PartialEq",
    status = "PostgresDatabaseStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresDatabaseSpec {
    pub name: String,
    pub database_owner: Option<PostgresDatabaseOwner>,
    pub connection: PostgresAdminConnectionReference,
}

impl HasPostgresAdminConnection for PostgresDatabase {
    fn get_connection(&self) -> &PostgresAdminConnectionReference {
        &self.spec.connection
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresDatabaseStatus {

}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PostgresDatabaseOwner {
    ManagedRole(PostgresRoleReference),
    Name(String),
}