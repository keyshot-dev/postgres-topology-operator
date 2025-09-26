use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::postgres_password::PostgresPassword;
use crate::types::{HasPostgresAdminConnection, PgBouncerReference, PostgresAdminConnectionReference};


#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "postgres.digizuite.com",
    version = "v1alpha1",
    kind = "PostgresRole",
    plural = "postgresroles",
    derive = "PartialEq",
    status = "PostgresRoleStatus",
    printcolumn = r#"{"name":"Role", "type":"string", "description":"Name of the role", "jsonPath":".role"}"#,
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresRoleSpec {
    pub role: String,
    pub password: PostgresPassword,
    pub register_in_pg_bouncer: Option<PgBouncerReference>,
    pub grant_role_to_admin_user: Option<bool>,
    pub connection: PostgresAdminConnectionReference,
    /// Any extra roles to grant this role, e.g. "admin"
    pub extra_roles: Option<Vec<String>>,
}

impl HasPostgresAdminConnection for PostgresRole {
    fn get_connection(&self) -> &PostgresAdminConnectionReference {
        &self.spec.connection
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresRoleStatus {
    pub encoded_password: Option<StatusEncodedPassword>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatusEncodedPassword {
    pub original: PostgresPassword,
    pub encoded: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresRoleReference {
    pub name: String,
    pub namespace: Option<String>,
}
