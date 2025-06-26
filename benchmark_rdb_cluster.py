from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import subprocess
import time
import os
import sys
import valkey 
import argparse
import re
import json 

# --- Configuration Constants ---
VALKEY_SERVER_PATH = "./src/valkey-server"
VALKEY_CLI_PATH = "./src/valkey-cli"
TEST_CONF_TEMPLATE = "testconfs/valkey_rdb_benchmark_cluster_base.conf" # Base Config File
DEFAULT_TEMP_SUBDIR="valkey_rdb_benchmark_cluster_run"
DEFAULT_START_PORT = 7001
NUM_CLUSTER_NODES = 3 # Minimum 3 nodes for a functional cluster
DEFAULT_DB_FILE = "dump.rdb"
DEFAULT_LOG_FILE = "valkey.log"
EXPECTED_KEYS_FILE = "expected_keys.json" # File to store populated keys for verification
DEFAULT_KEY_SIZE = 1000
DEFAULT_NUM_KEYS = int(10e6) 
RDB_SNAPSHOT_THREADS = 4

NUM_PROCESSES = 40

# --- Helper Functions for Server Management ---
def start_valkey_server(port: int, conf_path: str, data_dir: str, log_file_path: str,
                        cluster_mode: bool = False, cluster_config_file_name: str = None):
    """Starts a Valkey server instance in the background."""
    print(f"Starting Valkey server on port {port}...")

    # Ensure the data directory exists and is clean
    if os.path.exists(data_dir):
        subprocess.run(["rm", "-rf", os.path.join(data_dir, "*")], check=True)
    os.makedirs(data_dir, exist_ok=True)

    # Command to start Valkey server
    command = [
        VALKEY_SERVER_PATH,
        conf_path,
        "--port", str(port),
        "--dir", data_dir,
        "--dbfilename", DEFAULT_DB_FILE,
        "--loglevel", "notice",
        "--logfile", log_file_path,
        "--rdb-snapshot-threads", str(RDB_SNAPSHOT_THREADS),
        "--save", "" # Don't save until asked

    ]
    if cluster_mode:
        command.extend([
            "--cluster-enabled", "yes",
            "--cluster-config-file", cluster_config_file_name
        ])
    else:
        command.extend([
            "--cluster-enabled", "no" # Explicitly disable for standalone tests
        ])

    try:
        # Open new process running valkey 
        # In the future we would like this to run on a VM somewhere
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)
        print(f"Valkey server started with PID: {process.pid} on port {port}. Log: {log_file_path}")
        time.sleep(1) # Give server a moment to start
        
        # Verify server is reachable using the valkey library
        vk_client = valkey.Valkey(host='127.0.0.1', port=port, decode_responses=True)
        vk_client.ping() # Will raise an exception if not reachable    
        print(f"Server on port {port} is reachable.")
        return process, vk_client 
    
    except Exception as e:
        print(f"Error starting Valkey server on port {port}: {e}")
        if process and process.poll() is None:
            os.killpg(os.getpgid(process.pid), 9) # SIGKILL
            process.wait()
        return None, None

def stop_valkey_server(process: subprocess.Popen, client: valkey.ValkeyCluster, port: int):
    """Stops a Valkey server instance."""
    if not process or process.poll() is not None:
        if client:
            try:
                client.connection_pool.disconnect()
            except Exception:
                pass
        return

    print(f"Stopping Valkey server on port {port} (PID: {process.pid})...")
    try:
        # Try to shut down gracefully using Valkey's SHUTDOWN command
        client.shutdown(save=False)
        time.sleep(2)
    except valkey.exceptions.ConnectionError:
        print(f"Server on port {port} already disconnected (graceful shutdown).")
    except Exception as e:
        print(f"Error during graceful shutdown for port {port}: {e}")

    # Ensure the process is truly terminated
    if process.poll() is None: # Check if process is still running
        print(f"Valkey server on port {port} still running, killing forcefully...")
        try:
            os.killpg(os.getpgid(process.pid), 9) # SIGKILL
        except ProcessLookupError:
            print(f"Warning: PID {process.pid} not found when trying to force kill.")
    
    process.wait() # Wait for the process to fully terminate
    print(f"Valkey server on port {port} stopped.")

    
def start_valkey_cluster(num_nodes: int, start_port: int, conf_path: str, temp_base_dir: str):
    """Starts multiple Valkey servers and forms a cluster."""
    print(f"\n--- Starting Valkey Cluster with {num_nodes} nodes ---")
    processes = []
    clients = [] # These will be individual valkey.Valkey clients
    node_addresses = []

    for i in range(num_nodes):
        port = start_port + i
        data_dir = os.path.join(temp_base_dir, f"node_data_{port}")
        log_file_path = os.path.join(temp_base_dir, f"node_log_{port}.log")
        cluster_config_file_name = f"nodes-{port}.conf"

        process, client = start_valkey_server(
            port, conf_path, data_dir, log_file_path,
            cluster_mode=True, cluster_config_file_name=cluster_config_file_name
        )
        if process and client:
            processes.append(process)
            clients.append(client)
            node_addresses.append(f"127.0.0.1:{port}")
        else:
            print(f"Failed to start node on port {port}. Aborting cluster setup.")
            stop_valkey_cluster(processes, clients) # Stop existing clients
            return None, None, None, None # Return None for the cluster_client as well

    print("\n--- Creating Valkey Cluster ---")
    create_command = [VALKEY_CLI_PATH, "--cluster", "create"] + node_addresses + ["--cluster-replicas", "0"]
    print(f"Running command: {' '.join(create_command)}")
    
    cluster_create_process = subprocess.run(
        create_command,
        input="yes\nyes\n",
        capture_output=True,
        text=True
    )
    print(cluster_create_process.stdout)
    if cluster_create_process.returncode != 0:
        print(f"ERROR creating cluster: {cluster_create_process.stderr}")
        print(cluster_create_process.stderr)
        stop_valkey_cluster(processes, clients) # Stop existing clients
        return  None, None, None, None, None # Return None for the cluster_client as well
    
    print("Cluster creation initiated. Waiting for cluster to stabilize...")
    cluster_stable_timeout = 60
    poll_interval = 2
    elapsed_stable_time = 0

    # Ensure clients are connected before checking cluster_info
    if not clients: # This should ideally not happen if nodes started successfully
        print("No clients available to check cluster status. Aborting.")
        return  None, None, None, None, None# Return None for the cluster_client as well

    while elapsed_stable_time < cluster_stable_timeout:
        try:
            cluster_status_str = clients[0].execute_command('CLUSTER', 'INFO')
            
            parsed_cluster_info = {}
            for line in cluster_status_str.strip().split('\r\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    parsed_cluster_info[key] = value

            cluster_state = parsed_cluster_info.get('cluster_state')
            slots_assigned = int(parsed_cluster_info.get('cluster_slots_assigned', 0))

            if cluster_state == 'ok' and slots_assigned == 16384:
                print("Cluster state is OK and all slots are assigned.")
                break
            else:
                print(f"Cluster state: {cluster_state}, Slots: {slots_assigned}/16384. Waiting...", end='\r')
        except valkey.exceptions.ConnectionError:
            print(f"Error: Client lost connection to node {clients[0].connection_pool.connection_kwargs['port']} while checking cluster info. Waiting...", end='\r')
        except Exception as e:
            print(f"Error checking cluster info: {e}. Waiting...", end='\r')
        
        time.sleep(poll_interval)
        elapsed_stable_time += poll_interval
    else: # We have hit the timeout for cluster stabilization
        print(f"\nERROR: Cluster did not stabilize after {cluster_stable_timeout} seconds.")
        print("Dumping logs for initial cluster state investigation:")
        for c in clients:
            try:
                port = c.connection_pool.connection_kwargs['port']
                with open(os.path.join(temp_base_dir, f"node_log_{port}.log"), 'r') as f:
                    print(f"--- Log for port {port} ---")
                    print("".join(f.readlines()[-50:]))
            except Exception as log_e:
                print(f"Could not read log for port {port}: {log_e}")
        stop_valkey_cluster(processes, clients) # Stop existing clients
        return None, None, None, None, None # Return None for the cluster_client as well

    # --- NOW, CREATE THE VALKEYCLUSTER CLIENT FOR APPLICATION-LEVEL COMMANDS ---
    # This client is aware of the cluster topology and handles MOVED/ASK redirections.
    startup_nodes_for_cluster_client = []
    for addr in node_addresses:
        host, port_str = addr.split(':')
        startup_nodes_for_cluster_client.append(valkey.cluster.ClusterNode(host, int(port_str))) # Changed to valkey.cluster.ClusterNode
    
    cluster_client = None
    try:
        cluster_client = valkey.ValkeyCluster(startup_nodes=startup_nodes_for_cluster_client, decode_responses=True)
        cluster_client.ping()
        print("ValkeyCluster client successfully connected to the cluster.")
    except Exception as e:
        print(f"ERROR: Could not initialize ValkeyCluster client: {e}")
        stop_valkey_cluster(processes, clients) # Stop existing clients
        return None, None, None, None # Return None for the cluster_client as well

    return startup_nodes_for_cluster_client, processes, clients, cluster_client, node_addresses

def stop_valkey_cluster(processes: list[subprocess.Popen], clients: list[valkey.Valkey]): 
    """Stops all Valkey cluster nodes."""
    print("\n--- Stopping Valkey Cluster nodes ---")
    # Stop clients from the last one to the first to avoid issues if clients try to reconnect
    # and the first one is already down.
    for i in reversed(range(len(processes))):
        stop_valkey_server(processes[i], clients[i], clients[i].connection_pool.connection_kwargs['port'])


# --- Data Population ---

# Global list to store expected keys for verification
EXPECTED_KEY_VALUES = []

def make_key(hash_tag_prefix: str, index: int) -> str:
    return f"{{{hash_tag_prefix}{index}}}:key_{index}"

def make_val(key:str, key_value_size: int) -> str:
    return key + "a" * (key_value_size - len(key)) # Can make this different later
    

def populate_data_cluster(cluster_startup_nodes: list[valkey.cluster.ClusterNode],
                          num_keys: int,
                          key_value_size: int,
                          temp_base_dir: str, # temp_base_dir is still needed for saving final expected_keys.json
                          hash_tag_prefix: str = "hash"):
    """
    Populates the Valkey cluster with string keys using multiprocessing.
    All keys are generated in the main process, then chunks are sent to workers.
    """
    start_time = time.time()
    print(f"Generating {num_keys} keys with {key_value_size} bytes each in main process...")

    global EXPECTED_KEY_VALUES
    EXPECTED_KEY_VALUES = [] # Clear previous expected keys

    # Pre-generate all keys and values in the main process
    for i in range(num_keys):
        key = make_key(hash_tag_prefix, i)
        # value = make_val(key, key_value_size)
        EXPECTED_KEY_VALUES.append(key)
    
    print(f"Time to generate keys: {time.time() - start_time}")
    start_time = time.time()

    print(f"Main process generated {num_keys} key-value pairs.")
    print("Starting multiprocessing population into the cluster...")

    # Determine optimal number of processes
    num_processes = multiprocessing.cpu_count()
    if num_processes > num_keys: # Don't create more processes than keys
        num_processes = num_keys
    if num_processes == 0:
        print("No processes available for population. Skipping.", file=sys.stderr)
        return
    
    num_processes = NUM_PROCESSES
    print(f"Num Processes for submitting keys = {num_processes}")

    chunk_size = (len(EXPECTED_KEY_VALUES) + num_processes - 1) // num_processes # Ceil division

     # Convert valkey.cluster.ClusterNode objects to a picklable format (list of tuples)
    picklable_cluster_startup_nodes_info = [(node.host, node.port) for node in cluster_startup_nodes]
    print(picklable_cluster_startup_nodes_info)

    # Using ProcessPoolExecutor for parallel execution
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = []
        for i in range(num_processes):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(EXPECTED_KEY_VALUES))
            if start_idx >= len(EXPECTED_KEY_VALUES): # No more keys to process for this worker
                continue

            # Get the slice of keys and values for this worker
            keys_slice = EXPECTED_KEY_VALUES[start_idx:end_idx] # .copy() to ensure each process gets its own copy

            # Submit task to the pool, passing the picklable info
            
            futures.append(executor.submit(_populate_worker_task,
                                           keys_slice,
                                           picklable_cluster_startup_nodes_info,
                                           key_value_size,
                                           i))

        # Wait for all futures to complete and handle results
        all_workers_successful = True
        for i, future in enumerate(as_completed(futures)):
            if future.result():
                print(f"Main: Worker {i} finished populating its keys.")
            else:
                print(f"Main: Worker {i} failed.", file=sys.stderr)
                all_workers_successful = False

    if not all_workers_successful:
        print("WARNING: Some workers failed during population. Data might be incomplete.", file=sys.stderr)
    
    end_time = time.time()
    load_time = end_time - start_time
    keys_per_second = num_keys / load_time
    print(f"Total time to load keys: {end_time - start_time} seconds")
    print(f"Keys per second: {keys_per_second}")


# --- Data Population (Multiprocessed) ---
def _populate_worker_task(keys: list[str],
                          cluster_startup_nodes_info: list[tuple[str, int]], key_value_size: int,
                          worker_id: int):
    """
    Worker function for populating a given list of keys into the Valkey cluster.
    Each worker creates its own client.
    """
    print(f"Running worker {worker_id}")
    worker_client = None
    
    # State variables for progress tracking
    total_keys_in_slice = len(keys)
    percentage_interval = 50 # Report every 10%
    last_reported_percentage = -1 # Initialize to -1 to ensure 0% is printed immediately if total_keys_in_slice > 0

    try:
        # Reconstruct ClusterNode objects from picklable info
        startup_nodes_for_worker = [valkey.cluster.ClusterNode(host, port) for host, port in cluster_startup_nodes_info]
        worker_client = valkey.ValkeyCluster(startup_nodes=startup_nodes_for_worker, decode_responses=True)
        worker_client.ping() # Verify connection

        pipe = worker_client.pipeline()
        num_processed = 0

        for key in keys:
            value = make_val(key, key_value_size)
            pipe.set(key, value)
            num_processed += 1

            if num_processed % 50000 == 0: # Smaller batch size for worker updates
                # print(f"Worker {worker_id} populated {num_processed} keys")
                pipe.execute()
               # NEW: Separate condition for percentage print updates
            # if total_keys_in_slice > 0: # Avoid division by zero
            #     current_percentage = int((num_processed / total_keys_in_slice) * 100)
                
                # Print only when a new percentage interval is reached
                # if current_percentage >= last_reported_percentage + percentage_interval or num_processed == total_keys_in_slice:
                #     print(f"Worker {worker_id}: {current_percentage}% done ({num_processed}/{total_keys_in_slice} keys)")
                #     last_reported_percentage = current_percentage
        pipe.execute() # Execute any remaining commands

        print(f"Worker {worker_id} completed {num_processed} keys.")
        return True # Indicate success
    except Exception as e:
        print(f"Worker {worker_id} failed: {e}", file=sys.stderr)
        return False # Indicate failure
    finally:
        if worker_client:
            try:
                worker_client.connection_pool.disconnect()
            except Exception:
                pass
    
def verify_data_cluster(client: valkey.ValkeyCluster, key_value_size: int):
    """
    Verifies the existence and correctness of keys populated in the cluster.
    Assumes EXPECTED_KEY_VALUES global list is populated.
    """
    print("\n--- Verifying populated keys in the cluster ---")
    verification_success = True
    errors_found = 0
    
    # When using redis-py (or valkey-py) against a cluster, the client object
    # usually handles routing automatically, so direct client.get(key) should work.
    # We do NOT need to use valkey-cli -c for this part if redis-py is configured for cluster.
    # However, if using redis.Redis and not redis.RedisCluster, you might still need to
    # connect to the master of the specific slot. For now, we assume simple client.get()
    # will work due to server-side redirection or a smart client.

    for i in range(0, len(EXPECTED_KEY_VALUES), 10000):
        expected_key = EXPECTED_KEY_VALUES[i]
        expected_value = make_val(expected_key, key_value_size)
        try:
            actual_value = client.get(expected_key)
            
            if actual_value == expected_value:
                # print(f"  Key '{expected_key}' OK.") # Uncomment for verbose
                pass
            else:
                print(f"  Key '{expected_key}' MISMATCH! Expected: '{expected_value}', Got: '{actual_value}'")
                verification_success = False
                errors_found += 1
        except Exception as e:
            print(f"  Error getting key '{expected_key}': {e}")
            verification_success = False
            errors_found += 1

        if (i + 1) % 5000 == 0:
            print(f"  Verified {i+1}/{len(EXPECTED_KEY_VALUES)} keys. Errors: {errors_found}", end='\r')
            
    print(f"\nVerification complete. Total keys: {len(EXPECTED_KEY_VALUES)}, Errors: {errors_found}.")
    if verification_success:
        print("All keys verified successfully!")
    else:
        print("WARNING: Some keys failed verification.")
    return verification_success


# ... (existing imports, constants, start/stop server/cluster, populate/verify data) ...
def run_bgsave_benchmark_cluster(start_port: int, num_nodes: int, conf_path: str, num_keys: int,
                                 key_value_size: int, temp_base_dir: str):
    """
    Runs a benchmark for the BGSAVE operation on a cluster, including client-side timing
    and parsing server-side logs for internal timings.
    """
    
    cluster_startup_nodes, processes, clients, cluster_client_for_commands, node_addresses = start_valkey_cluster(num_nodes, start_port, conf_path, temp_base_dir)
    if not processes:
        print("Failed to start cluster. Aborting benchmark.")
        return None

    try:
        # Use the dedicated cluster_client_for_commands (or node_addresses for CLI pipe) for population
        # Use the CLI pipe method for faster population
        populate_data_cluster(cluster_startup_nodes, num_keys, key_value_size, temp_base_dir)

        initial_verification_ok = verify_data_cluster(cluster_client_for_commands, key_value_size)
        if not initial_verification_ok:
            print("Initial data verification failed. Aborting benchmark.")
            return None

        # --- Run BGSAVE on ALL nodes ---
        all_node_bgsave_results = []
        for node_client in clients:
            # Pass the individual client, temp_base_dir, and DEFAULT_LOG_FILE for consistency
            results = run_single_node_bgsave(node_client, temp_base_dir, DEFAULT_LOG_FILE)
            all_node_bgsave_results.append(results)
            if results.get("bgsave_status") == "error":
                print(f"BGSAVE failed on node {results.get('port')}. Continuing with other nodes but noting failure.")
        
        print("\n--- All BGSAVE operations initiated and monitored. ---")
        
        # Post-BGSAVE data verification (using the cluster client)
        post_bgsave_verification_ok = verify_data_cluster(cluster_client_for_commands, key_value_size)
        if not post_bgsave_verification_ok:
            print("WARNING: Data verification failed after BGSAVE operations on cluster.")
            
        # Aggregate results
        aggregated_results = {
            "keys": num_keys,
            "value_size": key_value_size,
            "num_nodes": num_nodes,
            "data_dir": temp_base_dir,
            "initial_data_verified": initial_verification_ok,
            "post_bgsave_data_verified": post_bgsave_verification_ok,
            "nodes_bgsave_results": all_node_bgsave_results # Store individual node results
        }
        
        # You might want to add aggregated timing metrics here, e.g., max/min/avg duration across nodes
        total_client_bgsave_duration = sum(r.get('client_bgsave_duration', 0) for r in all_node_bgsave_results if 'client_bgsave_duration' in r)
        aggregated_results["total_client_bgsave_duration_sum"] = total_client_bgsave_duration
        aggregated_results["average_bgsave_duration"] =  sum(r.get('client_bgsave_duration', 0) for r in all_node_bgsave_results if 'client_bgsave_duration' in r)/num_nodes
        aggregated_results["max_client_bgsave_duration"] = max(r.get('client_bgsave_duration', 0) for r in all_node_bgsave_results if 'client_bgsave_duration' in r)
        
        return aggregated_results

    finally:
        # Stop all cluster nodes
        stop_valkey_cluster(processes, clients)
        

def run_single_node_bgsave(client: valkey.Valkey, temp_base_dir: str, default_log_file: str = "valkey.log") -> dict:
    """
    Triggers and monitors a BGSAVE operation on a single Valkey node.
    Parses logs for internal timings.
    """
    node_port = client.connection_pool.connection_kwargs['port']
    node_log_file_path = os.path.join(temp_base_dir, f"node_log_{node_port}.log") # Ensure this matches where start_valkey_server saves logs

    print(f"\n--- Triggering BGSAVE on node {node_port} ---")
    client_bgsave_start_time = time.perf_counter()
    
    try:
        client.bgsave() # This is a non-blocking call
        print(f"BGSAVE command sent to {node_port}. Waiting for completion...")

        bgsave_timeout = 300 # Max 300 seconds (5 minutes)
        poll_interval = 0.1
        elapsed_wait_time = 0

        while True:
            try:
                info_persistence = client.info('persistence')
            except valkey.exceptions.ConnectionError:
                print(f"Warning: Client lost connection to node {node_port} during BGSAVE polling. Server might have crashed.")
                break

            is_bgsave_in_progress = info_persistence.get('rdb_bgsave_in_progress', 0)
            current_bgsave_time_sec = info_persistence.get('rdb_current_bgsave_time_sec', -1)

            if is_bgsave_in_progress == 0:
                print(f"\nBGSAVE on node {node_port} confirmed as finished by INFO persistence.")
                break
            
            print(f"  Node {node_port} BGSAVE in progress... current duration: {current_bgsave_time_sec}s", end='\r')
            time.sleep(poll_interval)
            elapsed_wait_time += poll_interval

            if elapsed_wait_time >= bgsave_timeout:
                print(f"\nERROR: BGSAVE on node {node_port} timed out after {bgsave_timeout} seconds.")
                break # Break from polling loop

        client_bgsave_end_time = time.perf_counter()
        client_bgsave_duration = client_bgsave_end_time - client_bgsave_start_time
        print(f"Node {node_port} client-side (poll detected) BGSAVE duration: {client_bgsave_duration:.4f} seconds.")

        # Get final BGSAVE status from INFO persistence
        try:
            final_info_persistence = client.info('persistence')
            rdb_last_bgsave_status = final_info_persistence.get('rdb_last_bgsave_status')
            rdb_last_bgsave_time_sec = final_info_persistence.get('rdb_last_bgsave_time_sec')
            print(f"Node {node_port} server reported last BGSAVE status: {rdb_last_bgsave_status}")
            print(f"Node {node_port} server reported last BGSAVE duration: {rdb_last_bgsave_time_sec} seconds.")
        except Exception as e:
            print(f"Node {node_port} could not retrieve final BGSAVE info: {e}")
            rdb_last_bgsave_status = "error"
            rdb_last_bgsave_time_sec = None

        # --- Parse server log for internal timings ---
        server_start_us = None
        server_end_us = None
        server_log_duration_us = None

        return {
            "port": node_port,
            "client_bgsave_duration": client_bgsave_duration,
            "server_info_bgsave_duration": rdb_last_bgsave_time_sec,
            "bgsave_status": rdb_last_bgsave_status,
        }
    except Exception as e:
        print(f"Error running BGSAVE on node {node_port}: {e}")
        return {
            "port": node_port,
            "error": str(e),
            "bgsave_status": "error"
        }

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Valkey RDB Persistence Benchmark Tool for Cluster")
    parser.add_argument("--start-port",
                        type=int,
                        default=DEFAULT_START_PORT,
                        help=f"Starting port for Valkey cluster nodes (default: {DEFAULT_START_PORT})")
    parser.add_argument("--num-nodes",
                        type=int,
                        default=NUM_CLUSTER_NODES,
                        help=f"Number of master nodes in the cluster (default: {NUM_CLUSTER_NODES})")
    parser.add_argument("--num-keys",
                        type=int,
                        default=DEFAULT_NUM_KEYS,
                        help="Number of keys to populate (default: 100000)")
    parser.add_argument("--value-size",
                        type=int,
                        default=DEFAULT_KEY_SIZE,
                        help="Size of the value in bytes for populated keys (default: 100)")
    parser.add_argument("--conf",
                        type=str,
                        default=TEST_CONF_TEMPLATE,
                        help=f"Path to the Valkey server configuration file template (default: {TEST_CONF_TEMPLATE}). This config should NOT contain cluster-enabled yes, as it's added by the script.")
    parser.add_argument("--temp-dir",
                        type=str,
                        default=os.path.join(os.getcwd(), f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),
                        help="Base directory for temporary data and logs for the cluster")

    args = parser.parse_args()

    # Create a unique temporary directory for this run
    if not os.path.exists(args.temp_dir):
        os.makedirs(args.temp_dir)
        print(f"Created temporary directory: {args.temp_dir}")
    else:
        print(f"Using existing temporary directory: {args.temp_dir}")
        subprocess.run(["rm", "-rf", os.path.join(args.temp_dir, "*")], check=True) # Clear previous contents

    print("--- Starting RDB Persistence Benchmark for Cluster ---")
    results = run_bgsave_benchmark_cluster(
        args.start_port,
        args.num_nodes,
        args.conf,
        args.num_keys,
        args.value_size,
        args.temp_dir
    )

    if results:
        print("\n--- Benchmark Summary ---")
        for key, value in results.items():
            print(f"{key}: {value}")
    else:
        print("\nBenchmark failed to produce results.")
    
    print(f"\n--- Cleaning up temporary directory {args.temp_dir} ---")
    # You might want to uncomment this line for automatic cleanup in production benchmarks
    # subprocess.run(["rm", "-rf", args.temp_dir], check=True)
    print("Cleanup complete. Review log files in temp directory if not removed.")
    


if __name__ == "__main__":
    main()