use std::sync::Arc;
use std::time::Duration;
use kube::{Api, ResourceExt};
use kube_runtime::controller::Action;
use crate::ContextData;
use crate::types::{PostgresRole, PostgresSchema, PostgresSchemaOwner};
use crate::Error;
use crate::reconcilers::finalizers::{ensure_finalizer, remove_finalizer};
use crate::reconcilers::helpers::get_postgres_connection;

pub async fn reconcile_postgres_schema(resource: Arc<PostgresSchema>, context: Arc<ContextData>) -> anyhow::Result<Action, Error> {
    run_reconciler(resource, context).await.map_err(|e| e.into())
}

async fn run_reconciler(resource: Arc<PostgresSchema>, context: Arc<ContextData>) -> anyhow::Result<Action> {
    info!("Reconciling postgres_schema {:?}", resource.metadata.name);

    if resource.metadata.deletion_timestamp.is_some() {
        info!("Deleting postgres role {:?}", resource.metadata.name);

        let pg_connection = get_postgres_connection(resource.as_ref(), context.kubernetes_client.clone()).await?;

        pg_connection.execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", resource.spec.schema), &[]).await?;

        remove_finalizer(resource.as_ref().clone(), context.kubernetes_client.clone()).await?;

        return Ok(Action::await_change());
    }


    let resource = ensure_finalizer(resource.as_ref().clone(), context.kubernetes_client.clone()).await?;

    let owner_name = match &resource.spec.schema_owner {
        None => None,
        Some(PostgresSchemaOwner::Name(n)) => Some(n.clone()),
        Some(PostgresSchemaOwner::ManagedRole(role_reference)) => {
            let ns = resource.namespace().expect("Resource should be namespaced");
            let ns = role_reference.namespace.as_ref().unwrap_or(&ns);


            let role_api: Api<PostgresRole> = Api::namespaced(context.kubernetes_client.clone(), ns);

            if let Some(role) = role_api.get_opt(&role_reference.name).await? {
                Some(role.spec.role.clone())
            } else {
                error!("Role {} not found", role_reference.name);
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        },
    };

    let pg_connection = get_postgres_connection(&resource, context.kubernetes_client.clone()).await?;

    let schema = &resource.spec.schema;

    match (pg_connection.query_opt(r#"
        SELECT
          r.rolname AS schema_owner
        FROM pg_catalog.pg_namespace AS n
        JOIN pg_catalog.pg_roles AS r ON n.nspowner = r.oid
        WHERE n.nspname = $1;
    "#, &[&schema]).await?, owner_name) {
        (Some(_), None) => {
            info!("Schema {} already exists with specific owner", schema);
        },
        (Some(schema_owner), Some(owner_name)) => {
            let current_owner: &str = schema_owner.get(0);
            if current_owner != owner_name {
                info!("Changing schema {} owner from {} to {}", schema, current_owner, owner_name);
                pg_connection.execute(&format!("ALTER SCHEMA {} OWNER TO {}", schema, owner_name), &[]).await?;
                info!("Schema {} owner changed to {}", schema, owner_name);
            } else {
                info!("Schema {} already exists with owner {}", schema, owner_name);
            }
        },
        (None, None) => {
            info!("Creating schema {} with specific owner", schema);
            pg_connection.execute(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema), &[]).await?;
            info!("Schema created with specific owner");
        },
        (None, Some(owner_name)) => {
            info!("Creating schema {} with owner {}", schema, owner_name);
            pg_connection.execute(&format!("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}", schema, owner_name), &[]).await?;
            info!("Schema {} created with owner {}", schema, owner_name);
        }
    }


    Ok(Action::await_change())
}