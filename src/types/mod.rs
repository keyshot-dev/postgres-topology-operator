mod postgres_schema;
mod postgres_admin_connection;
mod postgres_role;
mod pg_bouncer;
mod pg_bouncer_database;
mod pg_bouncer_user;
mod postgres_database;

use std::fmt::{Display, Formatter};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use postgres_schema::*;
pub use postgres_database::*;
pub use postgres_admin_connection::*;
pub use postgres_role::*;
pub use pg_bouncer::*;
pub use pg_bouncer_database::*;
pub use pg_bouncer_user::*;


#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PostgresSslMode {
    #[default]
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl Display for PostgresSslMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PostgresSslMode::Disable => "disable",
            PostgresSslMode::Allow => "allow",
            PostgresSslMode::Prefer => "prefer",
            PostgresSslMode::Require => "require",
            PostgresSslMode::VerifyCa => "verify-ca",
            PostgresSslMode::VerifyFull => "verify-full",
        };

        f.write_str(s)
    }
}

