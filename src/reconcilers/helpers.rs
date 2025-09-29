use crate::types::{HasPostgresAdminConnection, PostgresAdminConnection, PostgresSslMode};
use anyhow::{bail, Context};
use kube::Api;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::ops::{Deref, DerefMut};
use tokio::task::JoinHandle;

pub async fn get_postgres_connection(
    res: &impl HasPostgresAdminConnection,
    kubernetes_client: kube::Client,
) -> anyhow::Result<PostgresConnection> {
    let admin_conn = res.get_connection();

    let ns = res.namespace().expect("Resource should be namespaced");
    let ns = admin_conn.namespace.as_ref().unwrap_or(&ns);

    let api: Api<PostgresAdminConnection> = Api::namespaced(kubernetes_client, &ns);

    let admin_conn = api.get_opt(&admin_conn.name).await?;

    let admin_conn = if let Some(admin_conn) = admin_conn {
        admin_conn.spec
    } else {
        bail!("Could not find postgres admin connection kubernetes object");
    };

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));

    if let Some(custom_cert) = &admin_conn.custom_root_certificate {
        let certs = CertificateDer::pem_slice_iter(custom_cert.as_bytes());
        let mut cert_list = vec![];
        for rel in certs {
            let cert = rel.with_context(|| "Failed to parse custom root certificate")?;
            cert_list.push(cert);
        }
        let (added, ignored) = root_store.add_parsable_certificates(&cert_list);

        info!("Added {added} custom certificates, while ignoring {ignored}");
    }

    let tls_config_builder = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store);

    let tls_config = if let Some(tls_auth) = &admin_conn.client_certificate_authorization {
        let root = CertificateDer::pem_slice_iter(tls_auth.root_certificate.as_bytes());
        let client = CertificateDer::pem_slice_iter(tls_auth.client_certificate.as_bytes());
        let certs = client.chain(
            root,
        );
        let mut cert_list = vec![];
        for rel in certs {
            let cert = rel.with_context(|| "Failed to parse client certificates")?;
            cert_list.push(rustls::Certificate(cert.to_vec()));
        }

        let key = PrivateKeyDer::from_pem_slice(tls_auth.client_key.as_bytes())
            .with_context(|| "Failed to parse client key")?;

        tls_config_builder
            .with_client_auth_cert(cert_list, rustls::PrivateKey(key.secret_der().to_vec()))
            .with_context(|| "Failed to create TLS client config with client authentication")?
    } else {
        tls_config_builder.with_no_client_auth()
    };

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

    let mut connection_config = tokio_postgres::config::Config::new();
    connection_config
        .host(&admin_conn.host)
        .port(admin_conn.port)
        .user(&admin_conn.username)
        .channel_binding(
            admin_conn
                .channel_binding
                .unwrap_or(crate::types::ChannelBinding::Disable)
                .to_postgres_channel_binding(),
        )
        .dbname(&admin_conn.database)
        .ssl_mode(match admin_conn.ssl_mode {
            PostgresSslMode::Disable => tokio_postgres::config::SslMode::Disable,
            PostgresSslMode::Allow | PostgresSslMode::Prefer => {
                tokio_postgres::config::SslMode::Prefer
            }
            PostgresSslMode::Require | PostgresSslMode::VerifyCa | PostgresSslMode::VerifyFull => {
                tokio_postgres::config::SslMode::Require
            }
        });

    if let Some(password) = &admin_conn.password {
        connection_config.password(password.get_raw_text());
    }

    #[cfg(debug_assertions)]
    if admin_conn.host == "cockroachdb-public.northeurope.svc.cluster.local" {
        connection_config.hostaddr("100.71.138.13".parse().expect("Invalid ipv4 address"));
    }

    let (client, connection) = connection_config.connect(tls).await?;

    let connection_join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });


    Ok(PostgresConnection {
        connection_join_handle,
        client,
        admin_username: admin_conn.username,
        database: admin_conn.database.clone(),
    })
}

pub struct PostgresConnection {
    #[allow(dead_code)]
    connection_join_handle: JoinHandle<()>,
    client: tokio_postgres::Client,
    pub admin_username: String,
    pub database: String,
}

impl Deref for PostgresConnection {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for PostgresConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}
