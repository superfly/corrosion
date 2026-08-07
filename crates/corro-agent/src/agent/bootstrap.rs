use crate::agent::RANDOM_NODES_CHOICES;
use corro_types::{agent::SplitPool, config::DEFAULT_GOSSIP_PORT};

use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use std::{collections::HashSet, net::SocketAddr};
use tokio::task::block_in_place;
use tracing::{debug, error, warn};

/// Apply the user-provided set of bootstrap nodes
pub async fn generate_bootstrap(
    bootstrap: &[String],
    our_addr: SocketAddr,
    pool: &SplitPool,
) -> eyre::Result<Vec<SocketAddr>> {
    let mut addrs = match resolve_bootstrap(bootstrap, our_addr).await {
        Ok(addrs) => addrs,
        Err(e) => {
            warn!("could not resolve bootstraps, falling back to in-db nodes: {e}");
            HashSet::new()
        }
    };

    if addrs.is_empty() {
        // fallback to in-db nodes
        let conn = pool.read().await?;
        addrs = block_in_place(|| {
            let mut prepped =
                conn.prepare("SELECT address FROM __corro_members ORDER BY RANDOM() LIMIT 5")?;
            let node_addrs = prepped.query_map([], |row| row.get::<_, String>(0))?;
            Ok::<_, rusqlite::Error>(
                node_addrs
                    .flatten()
                    .flat_map(|addr| addr.parse())
                    .filter(|addr| match (our_addr, addr) {
                        (SocketAddr::V6(our_ip), SocketAddr::V6(ip)) if our_ip != *ip => true,
                        (SocketAddr::V4(our_ip), SocketAddr::V4(ip)) if our_ip != *ip => true,
                        _ => {
                            debug!("ignore node with addr: {addr}");
                            false
                        }
                    })
                    .collect(),
            )
        })?;
    }

    let mut rng = StdRng::from_os_rng();

    Ok(addrs
        .into_iter()
        .choose_multiple(&mut rng, RANDOM_NODES_CHOICES))
}

async fn resolve_bootstrap(
    bootstrap: &[String],
    our_addr: SocketAddr,
) -> eyre::Result<HashSet<SocketAddr>> {
    use hickory_resolver::{
        config::{NameServerConfig, ResolverConfig},
        Resolver,
    };

    let mut addrs = HashSet::new();

    if bootstrap.is_empty() {
        return Ok(addrs);
    }

    let system_resolver = match Resolver::builder_tokio()?.build() {
        Ok(sr) => Some(sr),
        Err(error) => {
            error!(%error, "failed to build system DNS resolver");
            None
        }
    };

    for s in bootstrap {
        if let Ok(addr) = s.parse() {
            addrs.insert(addr);
            continue;
        }

        debug!("attempting to resolve {s}");
        let mut host_port_dns_server = s.split('@');
        let mut host_port = host_port_dns_server.next().unwrap().split(':');
        let mut resolver = None;
        if let Some(dns_server) = host_port_dns_server.next() {
            debug!("attempting to use resolver: {dns_server}");
            let (ip, port) = if let Ok(addr) = dns_server.parse::<SocketAddr>() {
                (addr.ip(), addr.port())
            } else {
                (dns_server.parse()?, 53)
            };

            // ConnectionConfig is non_exhaustive, hence this ugly constructions
            let mut nsc = NameServerConfig::udp_and_tcp(ip);
            for cc in &mut nsc.connections {
                cc.port = port;
            }

            match Resolver::builder_with_config(
                ResolverConfig::from_parts(None, Vec::new(), vec![nsc]),
                hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
            )
            .build()
            {
                Ok(res) => {
                    debug!("using resolver: {dns_server}");
                    resolver = Some(res);
                }
                Err(error) => {
                    error!(%error, "failed to build DNS resolver");
                }
            }
        }

        let Some(hostname) = host_port.next() else {
            continue;
        };

        let Some(resolver) = resolver.as_ref().or(system_resolver.as_ref()) else {
            eyre::bail!("could not resolve '{hostname}', no resolvers available");
        };

        debug!("Resolving '{hostname}' to an IP");
        match resolver.lookup_ip(hostname).await {
            Ok(response) => {
                debug!("Successfully resolved things: {response:?}");
                let port: u16 = host_port
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(DEFAULT_GOSSIP_PORT);
                for addr in response.iter().map(|ip| SocketAddr::from((ip, port))) {
                    match (our_addr, addr) {
                        (SocketAddr::V4(our_ip), SocketAddr::V4(ip)) if our_ip != ip => {}
                        (SocketAddr::V6(our_ip), SocketAddr::V6(ip)) if our_ip != ip => {}
                        _ => {
                            debug!("ignore node with addr: {addr}");
                            continue;
                        }
                    }
                    addrs.insert(addr);
                }
            }
            Err(e) => {
                if !e.is_no_records_found() {
                    error!("could not resolve '{hostname}': {e}");
                    return Err(e.into());
                }
            }
        }
    }

    Ok(addrs)
}
