use std::{net::IpAddr, path::Path};

use tokio::io::AsyncWriteExt;
use tracing::info;

pub async fn generate_ca<P: AsRef<Path>>(output_path: P) -> eyre::Result<()> {
    let cert = corro_types::tls::generate_ca()?;

    let cert_path = output_path.as_ref().join("ca.pem");
    let key_path = output_path.as_ref().join("ca.key");

    let cert_pem = cert.serialize_pem();
    let mut cert_file = tokio::fs::File::create(&cert_path).await?;
    cert_file.write_all(cert_pem.unwrap().as_bytes()).await?;

    info!("Wrote CA cert to {}", cert_path.display());

    let private_key_pem = cert.serialize_private_key_pem();
    let mut private_key_file = tokio::fs::File::create(&key_path).await?;
    private_key_file
        .write_all(private_key_pem.as_bytes())
        .await?;

    info!("Wrote CA key to {}", key_path.display());

    Ok(())
}

pub async fn generate_server_cert<P1: AsRef<Path>, P2: AsRef<Path>>(
    ca_cert_path: P1,
    ca_key_path: P2,
    ip: IpAddr,
) -> eyre::Result<()> {
    let ca_key_bytes = tokio::fs::read(ca_key_path.as_ref()).await?;
    let ca_key_pem = String::from_utf8_lossy(&ca_key_bytes);

    let ca_cert_bytes = tokio::fs::read(ca_cert_path.as_ref()).await?;
    let ca_cert_pem = String::from_utf8_lossy(&ca_cert_bytes);

    let (cert, cert_signed) =
        corro_types::tls::generate_server_cert(&ca_cert_pem, &ca_key_pem, ip)?;

    let mut cert_file = tokio::fs::File::create("cert.pem").await?;
    cert_file.write_all(cert_signed.as_bytes()).await?;

    info!("Wrote server certificate to ./cert.pem");

    let private_key_pem = cert.serialize_private_key_pem();
    let mut private_key_file = tokio::fs::File::create("cert.key").await?;
    private_key_file
        .write_all(private_key_pem.as_bytes())
        .await?;

    info!("Wrote server key to ./cert.key");

    Ok(())
}

pub async fn generate_client_cert<P1: AsRef<Path>, P2: AsRef<Path>>(
    ca_cert_path: P1,
    ca_key_path: P2,
) -> eyre::Result<()> {
    let ca_key_bytes = tokio::fs::read(ca_key_path.as_ref()).await?;
    let ca_key_pem = String::from_utf8_lossy(&ca_key_bytes);

    let ca_cert_bytes = tokio::fs::read(ca_cert_path.as_ref()).await?;
    let ca_cert_pem = String::from_utf8_lossy(&ca_cert_bytes);

    let (cert, cert_signed) = corro_types::tls::generate_client_cert(&ca_cert_pem, &ca_key_pem)?;

    let mut cert_file = tokio::fs::File::create("client-cert.pem").await?;
    cert_file.write_all(cert_signed.as_bytes()).await?;

    info!("Wrote client certificate to ./client-cert.pem");

    let private_key_pem = cert.serialize_private_key_pem();
    let mut private_key_file = tokio::fs::File::create("client-cert.key").await?;
    private_key_file
        .write_all(private_key_pem.as_bytes())
        .await?;

    info!("Wrote client key to ./client-key.pem");

    Ok(())
}
