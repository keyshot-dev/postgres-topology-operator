use kube::{CustomResource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::postgres_password::PostgresPassword;
use crate::types::PostgresSslMode;


#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
group = "postgres.digizuite.com",
version = "v1alpha1",
kind = "PostgresAdminConnection",
plural = "postgresadminconnections",
derive = "PartialEq",
printcolumn = r#"{"name":"Host", "type":"string", "description":"Postgres host", "jsonPath":".host"}"#,
printcolumn = r#"{"name":"Database", "type":"string", "description":"Name of the database", "jsonPath":".database"}"#,
printcolumn = r#"{"name":"Username", "type":"string", "description":"Name of the admin user", "jsonPath":".username"}"#,
namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresAdminConnectionSpec {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: PostgresPassword,
    pub database: String,
    pub ssl_mode: PostgresSslMode,
    pub channel_binding: Option<ChannelBinding>,
    pub custom_root_certificate: Option<String>,
}


/// Channel binding configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ChannelBinding {
    /// Do not use channel binding.
    Disable,
    /// Attempt to use channel binding but allow sessions without.
    Prefer,
    /// Require the use of channel binding.
    Require,
}

impl ChannelBinding {
    pub fn to_postgres_channel_binding(self) -> tokio_postgres::config::ChannelBinding {
        match self {
            ChannelBinding::Disable => tokio_postgres::config::ChannelBinding::Disable,
            ChannelBinding::Prefer => tokio_postgres::config::ChannelBinding::Prefer,
            ChannelBinding::Require => tokio_postgres::config::ChannelBinding::Prefer,
        }
    }
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostgresAdminConnectionReference {
    pub name: String,
    pub namespace: Option<String>,
}


pub trait HasPostgresAdminConnection: ResourceExt {
    fn get_connection(&self) -> &PostgresAdminConnectionReference;
}