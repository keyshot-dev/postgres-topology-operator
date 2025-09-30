mod postgres_password;
mod types;
mod reconcilers;
mod helpers;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

use clap::Parser;
use futures::stream::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::client::Client;
use kube::{Api, CustomResourceExt, Resource};
use kube_runtime::controller::{Action};
use kube_runtime::watcher::Config;
use kube_runtime::{Controller};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use crate::types::{HasPgBouncerReference, PgBouncer, PgBouncerDatabase, PgBouncerUser, PostgresAdminConnection, PostgresDatabase, PostgresRole, PostgresSchema};

#[derive(Parser, Debug)]
#[command(long_about = None)]
struct Args {
    /// If the crd definitions should be written out
    #[arg(long, env = "GENERATE_CRDS")]
    generate_crds: bool,
}



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();


    info!("Build Timestamp: {}", env!("VERGEN_BUILD_TIMESTAMP"));
    info!("git hash: {}", env!("VERGEN_GIT_DESCRIBE"));

    let args = Args::parse();

    if args.generate_crds {
        write_crds()?;
    }

    let kubernetes_client = Client::try_default().await?;

    let pg_bouncer_api: Api<PgBouncer> = Api::all(kubernetes_client.clone());
    let related_pg_bouncer_databases_api: Api<PgBouncerDatabase> = Api::all(kubernetes_client.clone());
    let related_pg_bouncer_users_api: Api<PgBouncerUser> = Api::all(kubernetes_client.clone());
    let postgres_roles_api: Api<PostgresRole> = Api::all(kubernetes_client.clone());
    let postgres_schemas_api: Api<PostgresSchema> = Api::all(kubernetes_client.clone());
    let postgres_databases_api: Api<PostgresDatabase> = Api::all(kubernetes_client.clone());

    let deployments_api: Api<Deployment> = Api::all(kubernetes_client.clone());
    let services_api: Api<Service> = Api::all(kubernetes_client.clone());
    let config_map_api: Api<ConfigMap> = Api::all(kubernetes_client.clone());

    let context = Arc::new(ContextData {
        kubernetes_client: kubernetes_client.clone(),
    });

    let mut tasks = JoinSet::new();

    tasks.spawn(Controller::new(pg_bouncer_api.clone(), Config::default())
        .watches(related_pg_bouncer_databases_api, Config::default(), |o| o.get_pg_bouncer_object_ref())
        .watches(related_pg_bouncer_users_api.clone(), Config::default(), |o| o.get_pg_bouncer_object_ref())
        .owns(deployments_api, Config::default().labels("controller-watcher=postgres-topology-operator"))
        .owns(services_api, Config::default().labels("controller-watcher=postgres-topology-operator"))
        .owns(config_map_api, Config::default().labels("controller-watcher=postgres-topology-operator"))
        .run(reconcilers::pg_bouncer::reconcile_pg_bouncer, error_policy, context.clone())
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled: {}", o.0.name),
                Err(e) => error!("reconcile failed: {:?}", e),
            }
        }));
    //
    tasks.spawn(Controller::new(postgres_roles_api.clone(), Config::default())
        .owns(related_pg_bouncer_users_api, Config::default())
        .run(reconcilers::postgres_role::reconcile_postgres_role, error_policy, context.clone())
        .for_each(|res| async move {
            match res {
                Ok(o) => debug!("reconciled: {:?}", o),
                Err(e) => error!("reconcile failed: {:?}", e),
            }
        }));

    tasks.spawn(Controller::new(postgres_schemas_api.clone(), Config::default())
        .run(reconcilers::postgres_schema::reconcile_postgres_schema, error_policy, context.clone())
        .for_each(|res| async move {
            match res {
                Ok(o) => debug!("reconciled: {:?}", o),
                Err(e) => error!("reconcile failed: {:?}", e),
            }
        }));

    tasks.spawn(Controller::new(postgres_databases_api.clone(), Config::default())
        .run(reconcilers::postgres_database::reconcile_postgres_database, error_policy, context.clone())
        .for_each(|res| async move {
            match res {
                Ok(o) => debug!("reconciled: {:?}", o),
                Err(e) => error!("reconcile failed: {:?}", e),
            }
        }));

    info!("Operator tasks started");

    while let Some(res) = tasks.join_next().await {
        res?;
    }

    Ok(())
}


fn write_crds() -> anyhow::Result<()> {
    let file_path = "charts/postgres-topology-operator/templates/crds.yaml";

    let mut file = File::create(file_path)?;

    write_crd::<PostgresSchema>(&mut file)?;
    write_crd::<PostgresDatabase>(&mut file)?;
    write_crd::<PostgresAdminConnection>(&mut file)?;
    write_crd::<PostgresRole>(&mut file)?;
    write_crd::<PgBouncer>(&mut file)?;
    write_crd::<PgBouncerUser>(&mut file)?;
    write_crd::<PgBouncerDatabase>(&mut file)?;

    Ok(())
}

fn write_crd<TResource: CustomResourceExt>(mut file: &mut File) -> anyhow::Result<()> {
    let crd = TResource::crd();

    serde_yaml::to_writer(&mut file, &crd)?;
    write!(file, "\n---\n")?;

    Ok(())
}


pub struct ContextData {
    kubernetes_client: Client,
}

/// All errors possible to occur during reconciliation
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct Error(#[from] anyhow::Error);

fn error_policy<TResource>(
    echo: Arc<TResource>,
    error: &Error,
    _context: Arc<ContextData>,
) -> Action
    where
        TResource:
        Clone + Resource + CustomResourceExt + DeserializeOwned + Debug + Send + Sync + 'static,
{
    error!(
        "Reconciliation error while reconciling type {}: {:?} in {:?}\n{:?}.\n{:?}",
        TResource::crd_name(),
        echo.meta().name,
        echo.meta().namespace,
        error,
        echo
    );
    Action::requeue(Duration::from_secs(15))
}
