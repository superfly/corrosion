use std::{net::IpAddr, path::Path};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, DnValue, IsCa,
    KeyIdMethod, KeyPair, KeyUsagePurpose, SanType, PKCS_ECDSA_P384_SHA384,
};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use tracing::info;

pub async fn generate_ca() -> eyre::Result<()> {
    let mut params = CertificateParams::default();

    params.alg = &PKCS_ECDSA_P384_SHA384;
    params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P384_SHA384)?);
    params.key_identifier_method = KeyIdMethod::Sha384;

    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        DnValue::PrintableString("Corrosion Root CA".to_string()),
    );
    params.distinguished_name = dn;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365 * 5);

    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let cert = Certificate::from_params(params)?;

    let cert_pem = cert.serialize_pem();
    let mut cert_file = tokio::fs::File::create("ca.pem").await?;
    cert_file.write_all(cert_pem.unwrap().as_bytes()).await?;

    info!("Wrote CA cert to ./ca.pem");

    let private_key_pem = cert.serialize_private_key_pem();
    let mut private_key_file = tokio::fs::File::create("ca.key").await?;
    private_key_file
        .write_all(private_key_pem.as_bytes())
        .await?;

    info!("Wrote CA key to ./ca.key");

    Ok(())
}

pub async fn generate_server_cert<P1: AsRef<Path>, P2: AsRef<Path>>(
    ca_cert_path: P1,
    ca_key_path: P2,
    ip: IpAddr,
) -> eyre::Result<()> {
    let ca_key_bytes = tokio::fs::read(ca_key_path.as_ref()).await?;
    let ca_key_pair = KeyPair::from_pem(&String::from_utf8_lossy(&ca_key_bytes))?;

    let ca_cert_bytes = tokio::fs::read(ca_cert_path.as_ref()).await?;
    let ca_cert = Certificate::from_params(CertificateParams::from_ca_cert_pem(
        &String::from_utf8_lossy(&ca_cert_bytes),
        ca_key_pair,
    )?)?;

    let mut params = CertificateParams::default();

    params.alg = &PKCS_ECDSA_P384_SHA384;
    params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P384_SHA384).unwrap());
    params.key_identifier_method = KeyIdMethod::Sha384;

    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        DnValue::PrintableString("r.u.local".to_string()),
    );
    params.distinguished_name = dn;

    params.subject_alt_names = vec![SanType::IpAddress(ip)];

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365 * 1);

    let cert = Certificate::from_params(params)?;
    let cert_signed = cert.serialize_pem_with_signer(&ca_cert)?;

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
    let ca_key_pair = KeyPair::from_pem(&String::from_utf8_lossy(&ca_key_bytes))?;

    let ca_cert_bytes = tokio::fs::read(ca_cert_path.as_ref()).await?;
    let ca_cert = Certificate::from_params(CertificateParams::from_ca_cert_pem(
        &String::from_utf8_lossy(&ca_cert_bytes),
        ca_key_pair,
    )?)?;

    let mut params = CertificateParams::default();

    params.alg = &PKCS_ECDSA_P384_SHA384;
    params.key_pair = Some(KeyPair::generate(&PKCS_ECDSA_P384_SHA384).unwrap());
    params.key_identifier_method = KeyIdMethod::Sha384;

    let dn = DistinguishedName::new();
    params.distinguished_name = dn;

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365 * 1);

    let cert = Certificate::from_params(params)?;
    let cert_signed = cert.serialize_pem_with_signer(&ca_cert)?;

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
