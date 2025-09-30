use std::sync::Arc;
use std::time::Duration;
use kube::{Api, ResourceExt};
use kube_runtime::controller::Action;
use crate::ContextData;
use crate::types::{PostgresRole, PostgresDatabase, PostgresDatabaseOwner};
use crate::Error;
use crate::reconcilers::finalizers::{ensure_finalizer, remove_finalizer};
use crate::reconcilers::helpers::get_postgres_connection;

pub async fn reconcile_postgres_database(resource: Arc<PostgresDatabase>, context: Arc<ContextData>) -> anyhow::Result<Action, Error> {
    run_reconciler(resource, context).await.map_err(|e| e.into())
}

async fn run_reconciler(resource: Arc<PostgresDatabase>, context: Arc<ContextData>) -> anyhow::Result<Action> {
    info!("Reconciling postgres_database {:?}", resource.metadata.name);

    if resource.metadata.deletion_timestamp.is_some() {
        info!("Deleting postgres database {:?}", resource.metadata.name);

        let pg_connection = get_postgres_connection(resource.as_ref(), context.kubernetes_client.clone()).await?;

        pg_connection.execute(&format!("DROP DATABASE IF EXISTS {} CASCADE", resource.spec.name), &[]).await?;

        remove_finalizer(resource.as_ref().clone(), context.kubernetes_client.clone()).await?;

        return Ok(Action::await_change());
    }


    let resource = ensure_finalizer(resource.as_ref().clone(), context.kubernetes_client.clone()).await?;

    let owner_name = match &resource.spec.database_owner {
        None => None,
        Some(PostgresDatabaseOwner::Name(n)) => Some(n.clone()),
        Some(PostgresDatabaseOwner::ManagedRole(role_reference)) => {
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

    let database = &resource.spec.name;

    match (pg_connection.query_opt(r#"
        SELECT a.rolname
        FROM pg_catalog.pg_database d
        JOIN pg_catalog.pg_roles a ON d.datdba = a.oid
        WHERE d.datname = $1;
    "#, &[&database]).await?, owner_name) {
        (Some(_), None) => {
            info!("Database {} already exists with specific owner", database);
        },
        (Some(database_owner), Some(owner_name)) => {
            let current_owner: &str = database_owner.get(0);
            if current_owner != owner_name {
                info!("Changing database {} owner from {} to {}", database, current_owner, owner_name);
                pg_connection.execute(&format!("ALTER DATABASE {} OWNER TO {}", database, owner_name), &[]).await?;
                info!("Database {} owner changed to {}", database, owner_name);
            } else {
                info!("Database {} already exists with owner {}", database, owner_name);
            }
        }
        (None, None) => {
            info!("Creating database {} with specific owner", database);
            pg_connection.execute(&format!("CREATE DATABASE IF NOT EXISTS {}", database), &[]).await?;
            info!("Database created with specific owner");
        },
        (None, Some(owner_name)) => {
            info!("Creating database {} with owner {}", database, owner_name);
            pg_connection.execute(&format!("CREATE DATABASE IF NOT EXISTS {} OWNER = {}", database, owner_name), &[]).await?;
            info!("Database {} created with owner {}", database, owner_name);
        }
    }


    Ok(Action::await_change())
}