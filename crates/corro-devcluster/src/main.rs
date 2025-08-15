mod topology;

use clap::{Parser, Subcommand};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    process::Command,
    sync::mpsc::{channel, Sender},
    thread,
    time::Duration,
};

use crate::topology::Simple;

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    /// Set the state directory path.  If not set the environment
    /// variable CORRO_DEVCLUSTER_STATE_DIR will be used
    #[arg(long = "statedir", short = 'd', global = true)]
    state_directory: Option<PathBuf>,

    /// Set the state directory path.  If not set the environment
    /// variable CORRO_DEVCLUSTER_SCHEMA_DIR will be used
    #[arg(long = "schemadir", short = 's', global = true)]
    schema_directory: Option<PathBuf>,

    /// Provide the binary path for corrosion.  If none is provided,
    /// corrosion will be built with nix (which may take a minute)
    #[arg(long = "binpath", short = 'b', global = true)]
    binary_path: Option<String>,

    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Create a simple topology in format `A -> B`, `B -> C`, etc
    Simple {
        /// Set the topology file path
        topology_path: PathBuf,
    },
}

fn main() {
    let cli: Cli = Cli::parse();

    let state_dir = match cli
        .state_directory
        .or(env::var("CORRO_DEVCLUSTER_STATE_DIR")
            .ok()
            .map(|path| PathBuf::new().join(path)))
    {
        Some(dir) => dir,
        None => {
            eprintln!("FAILED: either pass `--statedir` or set 'CORRO_DEVCLUSTER_STATE_DIR' environment variable!");
            std::process::exit(1);
        }
    };

    let schema_dir = match cli
        .schema_directory
        .or(env::var("CORRO_DEVCLUSTER_SCHEMA_DIR")
            .ok()
            .map(|path| PathBuf::new().join(path)))
    {
        Some(dir) => dir,
        None => {
            eprintln!("FAILED: either pass `--statedir` or set 'CORRO_DEVCLUSTER_STATE_DIR' environment variable!");
            std::process::exit(1);
        }
    };

    let bin_path = cli
        .binary_path
        .or_else(|| build_corrosion().map(|h| h.path))
        .expect("failed to determine corrosion binary location!");

    match cli.command {
        CliCommand::Simple { topology_path } => {
            let mut topo_config = File::open(topology_path).expect("failed to open topology-file!");
            let mut topo_buffer = String::new();
            topo_config
                .read_to_string(&mut topo_buffer)
                .expect("failed to read topology-file!");

            let mut topology = Simple::default();
            topo_buffer.lines().for_each(|line| {
                topology
                    .parse_edge(line)
                    .expect("Syntax error in topology-file!");
            });

            run_simple_topology(topology, bin_path, state_dir, schema_dir);
        }
    }

    // let handle = build_corrosion(env::args().next().map(|s| PathBuf::new().join(s)).unwrap());
    // println!("{:#?}", handle);
}

fn run_simple_topology(topo: Simple, bin_path: String, state_dir: PathBuf, schema_dir: PathBuf) {
    println!("//// Creating topology: \n{topo:#?}");
    let nodes = topo.get_all_nodes();

    let mut port_map = BTreeMap::default();

    // First go assign ports to all the nodes
    for node_name in &nodes {
        // Generate a port in range 1025 - 32768
        let node_port: u16 = 1025 + rand::random::<u16>() % (32 * 1024) - 1025;
        port_map.insert(node_name.clone(), node_port);
    }

    // Then generate each config with the appropriate bootstrap_set
    for node_name in &nodes {
        let node_port = port_map.get(node_name).unwrap(); // We just put it there
        let node_state = state_dir.join(node_name);

        // Delete / create the node state directory
        let _ = std::fs::remove_dir(&node_state);
        let _ = std::fs::create_dir_all(&node_state);

        let mut bootstrap_set = vec![];
        for link in topo.inner.get(node_name).unwrap() {
            bootstrap_set.push(format!(
                "\"[::1]:{}\"", // only connect locally
                port_map.get(link).expect("Port for node not set!")
            ));
        }

        let node_config = generate_config(
            node_state.to_str().unwrap(),
            schema_dir.to_str().unwrap(),
            *node_port,
            bootstrap_set,
        );

        println!("Generated config for node '{node_name}': \n{node_config}",);

        let mut config_file = File::create(node_state.join("config.toml"))
            .expect("failed to create node config file");
        config_file
            .write_all(node_config.as_bytes())
            .expect("failed to write node config file");
    }

    let (tx, rx) = channel::<()>();

    // Spawn nodes those without bootstraps first if they exist.
    for (pure_responder, _) in topo.inner.iter().filter(|(_, vec)| vec.is_empty()) {
        run_corrosion(tx.clone(), bin_path.clone(), state_dir.join(pure_responder));
        thread::sleep(Duration::from_millis(250)); // give the start thread a bit of time to breathe
    }

    for (initiator, _) in topo.inner.iter().filter(|(_, vec)| !vec.is_empty()) {
        run_corrosion(tx.clone(), bin_path.clone(), state_dir.join(initiator));
        thread::sleep(Duration::from_millis(250)); // give the start thread a bit of time to breathe
    }

    // wait for the threads
    while let Ok(()) = rx.recv() {}
    Command::new("pkill")
        .arg("corrosion")
        .output()
        .expect("failed to gracefully kill corrosions. They've become sentient!!!");
}

fn generate_config(
    state_dir: &str,
    schema_dir: &str,
    port: u16,
    bootstrap_set: Vec<String>,
) -> String {
    let bootstrap = bootstrap_set.join(",");
    format!(
        r#"[db]
path = "{state_dir}/corrosion.db"
schema_paths = ["{schema_dir}"]

[gossip]
addr = "[::]:{port}"
external_addr = "[::1]:{port}"
bootstrap = [{bootstrap}]
plaintext = true

[api]
addr = "127.0.0.1:{api_port}"

[admin]
path = "{state_dir}/admin.sock"
"#,
        state_dir = state_dir,
        schema_dir = schema_dir,
        port = port,
        // the chances of a collision here are very very small since
        // every port is random
        api_port = port + 1,
        bootstrap = bootstrap
    )
}

#[derive(Debug)]
struct BinHandle {
    path: String,
}

fn nix_output(vec: &[u8]) -> Vec<HashMap<String, serde_json::Value>> {
    serde_json::from_slice(vec).unwrap()
}

fn run_corrosion(tx: Sender<()>, bin_path: String, state_path: PathBuf) {
    let node_log = File::create(state_path.join("node.log")).expect("couldn't create log file");
    let mut cmd = Command::new(bin_path);

    cmd.args([
        "-c",
        state_path.join("config.toml").to_str().unwrap(),
        "agent",
    ]);

    cmd.stdout(node_log);
    let mut cmd_handle = cmd.spawn().expect("failed to spawn corrosion!");

    thread::spawn(move || {
        println!("Waiting for node...");
        cmd_handle
            .wait()
            .expect("corrosion node has encountered an error!");
        tx.send(()).unwrap();
        println!("Node completed")
    });
}

fn build_corrosion() -> Option<BinHandle> {
    println!("Running 'nix build' ...");
    let build_output = Command::new("nix")
        .args(["build", "--json"])
        .output()
        .ok()?;

    let json = nix_output(&build_output.stdout).remove(0);

    Some(BinHandle {
        path: json
            .get("outputs")?
            .get("out")?
            .to_string()
            .replace('"', ""),
    })
}
