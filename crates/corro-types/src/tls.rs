use std::net::IpAddr;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, DnValue, IsCa,
    KeyIdMethod, KeyPair, KeyUsagePurpose, SanType, PKCS_ECDSA_P384_SHA384,
};
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Rcgen(#[from] rcgen::RcgenError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub fn generate_ca() -> Result<Certificate, Error> {
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

    Ok(cert)
}

pub fn generate_server_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    ip: IpAddr,
) -> Result<(Certificate, String), Error> {
    let ca_cert = ca_cert(ca_cert_pem, ca_key_pem)?;

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

    Ok((cert, cert_signed))
}

fn ca_cert(ca_cert_pem: &str, ca_key_pem: &str) -> Result<Certificate, rcgen::RcgenError> {
    Certificate::from_params(CertificateParams::from_ca_cert_pem(
        ca_cert_pem,
        KeyPair::from_pem(ca_key_pem)?,
    )?)
}

pub fn generate_client_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
) -> Result<(Certificate, String), Error> {
    let ca_cert = ca_cert(ca_cert_pem, ca_key_pem)?;

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

    Ok((cert, cert_signed))
}
