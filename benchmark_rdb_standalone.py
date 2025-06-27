from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import subprocess
import time
import os
import sys
import valkey  # Assuming valkey-py is installed
import argparse
import re
import json

# --- Configuration Constants ---
VALKEY_SERVER_PATH = "./src/valkey-server"
VALKEY_CLI_PATH = "./src/valkey-cli"
TEST_CONF_TEMPLATE = (
    "testconfs/valkey_rdb_benchmark_base.conf"  # Assumes a non-cluster config
)
DEFAULT_TEMP_SUBDIR = "valkey_rdb_benchmark_standalone_run"
DEFAULT_START_PORT = 7001
DEFAULT_DB_FILE = "dump.rdb"
DEFAULT_LOG_FILE = "valkey.log"
EXPECTED_KEYS_FILE = (
    "expected_keys.json"  # File to store populated keys for verification
)
DEFAULT_KEY_SIZE = 1000
DEFAULT_NUM_KEYS = int(
    50
)  # Reduced for quicker testing, can be changed back to 100e6
RDB_SNAPSHOT_THREADS = 2

NUM_PROCESSES = 30  # Number of Python processes for parallel key loading

# Global list to store expected keys for verification
EXPECTED_KEY_VALUES = []


# --- Helper Functions for Server Management ---
def start_valkey_server(
    port: int,
    conf_path: str,
    data_dir: str,
    log_file_path: str,
    extra_sleep_time: int = 0,
):
    """
    Starts a Valkey server instance in the background.
    Note: For standalone, cluster_mode and cluster_config_file_name are not needed.
    """
    print(f"Starting Valkey server on port {port}...")

    # Ensure the data directory exists and is clean
    if os.path.exists(data_dir):
        subprocess.run(["rm", "-rf", os.path.join(data_dir, "*")], check=True)
    os.makedirs(data_dir, exist_ok=True)

    # Command to start Valkey server
    command = [
        VALKEY_SERVER_PATH,
        conf_path,
        "--port",
        str(port),
        "--dir",
        data_dir,
        "--dbfilename",
        DEFAULT_DB_FILE,
        "--loglevel",
        "notice",
        "--logfile",
        log_file_path,
        "--rdb-snapshot-threads",
        str(RDB_SNAPSHOT_THREADS),
        "--save",
        "",  # Don't save until asked
        "--cluster-enabled",
        "no",  # Explicitly disable for standalone tests
    ]

    process = None
    try:
        # Open new process running valkey, allowing stdout/stderr for initial debugging
        # For production, you might redirect stdout/stderr to a log file.
        process = subprocess.Popen(command, preexec_fn=os.setsid)
        print(
            f"Valkey server started with PID: {process.pid} on port {port}. Log: {log_file_path}"
        )

        # Polling loop to wait for the server to become reachable
        max_retries = 10
        for i in range(max_retries):
            try:
                vk_client = valkey.Valkey(
                    host="127.0.0.1", port=port, decode_responses=True
                )
                vk_client.ping()  # Will raise an exception if not reachable
                print(f"Server on port {port} is reachable.")
                return (
                    process,
                    vk_client,
                    ("127.0.0.1", port),
                )  # Return client and picklable host/port tuple
            except valkey.exceptions.ConnectionError as ce:
                if i < max_retries - 1:
                    print(
                        f"Waiting for Valkey server on port {port} to start (attempt {i + 1}/{max_retries})... {ce}"
                    )
                    time.sleep(1)  # Wait a bit longer between retries
                    if extra_sleep_time > 0:
                        time.sleep(extra_sleep_time)
                else:
                    raise  # Re-raise the exception if all retries fail
    except Exception as e:
        print(f"Error starting Valkey server on port {port}: {e}", file=sys.stderr)
        if process and process.poll() is None:
            os.killpg(os.getpgid(process.pid), 9)  # SIGKILL
            process.wait()
        return None, None, None  # Return None for all return values


def stop_valkey_server(process: subprocess.Popen, client: valkey.Valkey, port: int):
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
        time.sleep(1)  # Give server a moment to shut down
    except valkey.exceptions.ConnectionError:
        print(f"Server on port {port} already disconnected (graceful shutdown).")
    except Exception as e:
        print(f"Error during graceful shutdown for port {port}: {e}", file=sys.stderr)

    # Ensure the process is truly terminated
    if process.poll() is None:  # Check if process is still running
        print(f"Valkey server on port {port} still running, killing forcefully...")
        try:
            os.killpg(os.getpgid(process.pid), 9)  # SIGKILL
        except ProcessLookupError:
            print(f"Warning: PID {process.pid} not found when trying to force kill.")

    process.wait()  # Wait for the process to fully terminate
    print(f"Valkey server on port {port} stopped.")


# --- Data Population Helpers ---
def make_key(hash_tag_prefix: str, index: int) -> str:
    """Generates a key with a hash tag. Hash tags are good practice even for standalone for consistency."""
    return f"{{{hash_tag_prefix}{index % 10}}}:key_{index}"


def make_val(key: str, key_value_size: int) -> str:
    """Generates a value of a specific size, starting with the key."""
    # Ensure the value is exactly key_value_size, padding with 'a' if necessary
    # Note: If key is longer than key_value_size, it will be truncated
    value = (key + "a" * (key_value_size - len(key)))[:key_value_size]
    assert len(value) == key_value_size, (
        f"Value length mismatch: {len(value)} != {key_value_size}"
    )
    return value


# --- Data Population (Multiprocessed) ---
def _populate_worker_task(
    keys: list[str],
    node_connection_info: tuple[str, int],  # Single (host, port) tuple for standalone
    key_value_size: int,
    worker_id: int,
):
    """
    Worker function for populating a given list of keys into the Valkey instance.
    Each worker creates its own regular Valkey client.
    """
    # print(f"Running worker {worker_id}")
    worker_client = None

    try:
        host, port = node_connection_info
        worker_client = valkey.Valkey(host=host, port=port, decode_responses=True)
        worker_client.ping()  # Verify connection

        pipe = worker_client.pipeline()
        num_processed = 0
        for key in keys:
            value = make_val(key, key_value_size)
            pipe.set(key, value)
            num_processed += 1

            # Execute pipeline batch every 50,000 keys
            if num_processed % 50000 == 0:
                pipe.execute()

        pipe.execute()  # Execute any remaining commands after the loop (important for last batch)

        print(f"\nWorker {worker_id} completed {num_processed} keys total.")
        return True  # Indicate success
    except Exception as e:
        print(f"\nWorker {worker_id} failed: {e}", file=sys.stderr)
        return False  # Indicate failure
    finally:
        if worker_client:
            try:
                worker_client.connection_pool.disconnect()
            except Exception:
                pass


def populate_data_standalone(
    node_connection_info: tuple[str, int],  # Now just a single (host, port) tuple
    num_keys: int,
    key_value_size: int,
    temp_base_dir: str,  # temp_base_dir is still needed for saving final expected_keys.json
    hash_tag_prefix: str = "hash",
):
    """
    Populates the Valkey standalone instance with string keys using multiprocessing.
    All keys are generated in the main process, then chunks are sent to workers.
    """
    start_time = time.time()
    print(
        f"Generating {num_keys} keys with {key_value_size} bytes each in main process..."
    )

    global EXPECTED_KEY_VALUES
    EXPECTED_KEY_VALUES = []  # Clear previous expected keys

    # Pre-generate all keys and values in the main process
    for i in range(num_keys):
        key = make_key(hash_tag_prefix, i)
        # value = make_val(key, key_value_size)
        EXPECTED_KEY_VALUES.append(key)

    print(f"Time to generate keys: {time.time() - start_time:.4f} seconds")
    start_time = time.time()

    print(f"Main process generated {num_keys} key-value pairs.")
    print("Starting multiprocessing population into Valkey...")

    num_processes = NUM_PROCESSES if num_keys > NUM_PROCESSES else num_keys
    print(f"Num Processes for submitting keys = {num_processes}")

    chunk_size = (
        len(EXPECTED_KEY_VALUES) + num_processes - 1
    ) // num_processes  # Ceil division

    # Using ProcessPoolExecutor for parallel execution
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = []
        for i in range(num_processes):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(EXPECTED_KEY_VALUES))
            if start_idx >= len(
                EXPECTED_KEY_VALUES
            ):  # No more keys to process for this worker
                continue

            keys_slice = EXPECTED_KEY_VALUES[start_idx:end_idx]

            # Submit task to the pool, passing the single node's picklable info
            futures.append(
                executor.submit(
                    _populate_worker_task,
                    keys_slice,
                    node_connection_info,  # Pass single (host, port)
                    key_value_size,
                    i,
                )
            )

        # Wait for all futures to complete and handle results
        all_workers_successful = True
        for future in as_completed(futures):
            if future.result():
                pass  # Worker task already prints its completion message
            else:
                print(
                    f"Main: A worker failed during population. Check stderr for details.",
                    file=sys.stderr,
                )
                all_workers_successful = False

    if not all_workers_successful:
        print(
            "WARNING: Some workers failed during population. Data might be incomplete.",
            file=sys.stderr,
        )

    end_time = time.time()
    load_time = end_time - start_time
    keys_per_second = num_keys / load_time
    print(f"Total time to load keys: {load_time:.4f} seconds")
    print(f"Keys per second: {keys_per_second:.2f} keys/sec")
    print(
        f"Data population complete. Total keys populated: {len(EXPECTED_KEY_VALUES)}."
    )


def verify_data_standalone(
    client: valkey.Valkey, key_value_size
):  # Now accepts a regular Valkey client
    """
    Verifies the existence and correctness of keys populated in the Valkey standalone instance.
    Assumes EXPECTED_KEY_VALUES global list is populated by populate_data_standalone.
    """
    print("\n--- Verifying populated keys in Valkey ---")
    verification_success = True
    errors_found = 0

    if not EXPECTED_KEY_VALUES:
        print(
            "No expected keys found to verify. Population might have failed or not run.",
            file=sys.stderr,
        )
        return False

    print(f"Starting verification of {len(EXPECTED_KEY_VALUES)} keys...")

    batch_size = 5000  # Optimal batch size may vary, adjust as needed

    for i in range(0, len(EXPECTED_KEY_VALUES), batch_size):
        if i % 10000 == 0:
            continue
        keys_only_batch = EXPECTED_KEY_VALUES[i : i + batch_size]

        try:
            actual_values = client.mget(keys_only_batch)  # Use mget for batch retrieval

            for i, key in enumerate(keys_only_batch):
                expected_value = make_val(key, key_value_size)

                if expected_value == actual_values[i]:
                    pass  # Key OK
                else:
                    # print(
                    #     f"  Key '{key}' MISMATCH! Expected: '{expected_value}', Got: '{actual_values[i]}'"
                    # )
                    verification_success = False
                    errors_found += 1
        except Exception as e:
            print(
                f"  Error during MGET for batch (starting with {keys_only_batch[0] if keys_only_batch else 'N/A'}): {e}"
            )
            verification_success = False
            errors_found += len(keys_only_batch)  # Assume all in batch failed

        # Progress update
        if (i + batch_size) % (50000) == 0:  # Print update for every ~50k keys verified
            print(
                f"  Verified {i + batch_size}/{len(EXPECTED_KEY_VALUES)} keys. Errors: {errors_found}",
                end="\r",
            )

    print(
        f"\nVerification complete. Total keys: {len(EXPECTED_KEY_VALUES)}, Errors: {errors_found}."
    )
    if verification_success:
        print("All keys verified successfully!")
    else:
        print("WARNING: Some keys failed verification.")
    return verification_success


def test_rdb_reload_and_verify(
    initial_process: subprocess.Popen,
    initial_client: valkey.Valkey,
    start_port: int,
    conf_path: str,
    key_value_size: int,
    temp_base_dir: str,
) -> tuple[subprocess.Popen | None, valkey.Valkey | None, bool]:
    """
    Shuts down the Valkey server, restarts it to load from RDB, and verifies keys.
    Returns the new process, new client, and verification success status.
    """
    print("\n--- Shutting down Valkey for RDB reload test ---")
    # Ensure the initial client is disconnected before stopping the process
    if initial_client:
        try:
            initial_client.connection_pool.disconnect()
        except Exception:
            pass
    stop_valkey_server(initial_process, initial_client, start_port)
    time.sleep(1)

    print("\n--- Restarting Valkey to load from RDB ---")
    # Start the server again. It should automatically load dump.rdb
    new_log_file = os.path.join(temp_base_dir, f"node_log_{start_port}_restart.log")
    restarted_process, restarted_client, restarted_node_info = start_valkey_server(
        start_port,
        conf_path,
        os.path.join(temp_base_dir, f"node_data_{start_port}"),
        new_log_file,
        extra_sleep_time=10,
    )

    if not restarted_process:
        print(
            "Failed to restart Valkey server for RDB load. RDB reload test aborted.",
            file=sys.stderr,
        )
        return None, None, False

    # Verify data after restart
    print("\n--- Verifying keys after RDB reload ---")
    post_reload_verification_ok = verify_data_standalone(
        restarted_client, key_value_size
    )
    if not post_reload_verification_ok:
        print(
            "WARNING: Data verification failed after RDB reload operation.",
            file=sys.stderr,
        )

    return restarted_process, restarted_client, post_reload_verification_ok


def get_db_key_count(port: int):
    try:
        client = valkey.Valkey(host="127.0.0.1", port=port, decode_responses=True)
        db_size = client.dbsize()
    except valkey.exceptions.ConnectionError as e:
        print(
            f"Error connecting to Valkey on port {port} to get DB size: {e}",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"An unexpected error occurred while getting DB size on port {port}: {e}",
            file=sys.stderr,
        )
    return db_size


def run_single_node_bgsave(
    client: valkey.Valkey, temp_base_dir: str, default_log_file: str = "valkey.log"
) -> dict:
    """
    Triggers and monitors a BGSAVE operation on a single Valkey node.
    Parses logs for internal timings.
    """
    node_port = client.connection_pool.connection_kwargs["port"]
    node_log_file_path = os.path.join(
        temp_base_dir, f"node_log_{node_port}.log"
    )  # Ensure this matches where start_valkey_server saves logs

    print(f"\n--- Triggering BGSAVE on node {node_port} ---")
    client_bgsave_start_time = time.perf_counter()

    try:
        client.bgsave()  # This is a non-blocking call
        print(f"BGSAVE command sent to {node_port}. Waiting for completion...")

        bgsave_timeout = 300  # Max 300 seconds (5 minutes)
        poll_interval = 0.1
        elapsed_wait_time = 0

        while True:
            try:
                info_persistence = client.info("persistence")
            except valkey.exceptions.ConnectionError:
                print(
                    f"Warning: Client lost connection to node {node_port} during BGSAVE polling. Server might have crashed."
                )
                break

            is_bgsave_in_progress = info_persistence.get("rdb_bgsave_in_progress", 0)
            current_bgsave_time_sec = info_persistence.get(
                "rdb_current_bgsave_time_sec", -1
            )

            if is_bgsave_in_progress == 0:
                print(
                    f"\nBGSAVE on node {node_port} confirmed as finished by INFO persistence."
                )
                break

            print(
                f"  Node {node_port} BGSAVE in progress... current duration: {current_bgsave_time_sec}s",
                end="\r",
            )
            time.sleep(poll_interval)
            elapsed_wait_time += poll_interval

            if elapsed_wait_time >= bgsave_timeout:
                print(
                    f"\nERROR: BGSAVE on node {node_port} timed out after {bgsave_timeout} seconds."
                )
                break  # Break from polling loop

        client_bgsave_end_time = time.perf_counter()
        client_bgsave_duration = client_bgsave_end_time - client_bgsave_start_time
        print(
            f"Node {node_port} client-side (poll detected) BGSAVE duration: {client_bgsave_duration:.4f} seconds."
        )

        # Get final BGSAVE status from INFO persistence
        try:
            final_info_persistence = client.info("persistence")
            rdb_last_bgsave_status = final_info_persistence.get(
                "rdb_last_bgsave_status"
            )
            rdb_last_bgsave_time_sec = final_info_persistence.get(
                "rdb_last_bgsave_time_sec"
            )
            print(
                f"Node {node_port} server reported last BGSAVE status: {rdb_last_bgsave_status}"
            )
            print(
                f"Node {node_port} server reported last BGSAVE duration: {rdb_last_bgsave_time_sec} seconds."
            )
        except Exception as e:
            print(f"Node {node_port} could not retrieve final BGSAVE info: {e}")
            rdb_last_bgsave_status = "error"
            rdb_last_bgsave_time_sec = None

        return {
            "port": node_port,
            "client_bgsave_duration": client_bgsave_duration,
            "server_info_bgsave_duration": rdb_last_bgsave_time_sec,
            "bgsave_status": rdb_last_bgsave_status,
        }
    except Exception as e:
        print(f"Error running BGSAVE on node {node_port}: {e}", file=sys.stderr)
        return {"port": node_port, "error": str(e), "bgsave_status": "error"}


def run_bgsave_benchmark_standalone(
    start_port: int,
    conf_path: str,
    num_keys: int,
    key_value_size: int,
    temp_base_dir: str,
):
    """
    Runs a benchmark for the BGSAVE operation on a single Valkey standalone instance,
    including client-side timing and parsing server-side logs.
    """
    # Create a unique temporary directory for this run
    if not os.path.exists(temp_base_dir):
        os.makedirs(temp_base_dir)
        print(f"Created temporary directory: {temp_base_dir}")
    else:
        print(f"Using existing temporary directory: {temp_base_dir}")
        subprocess.run(
            ["rm", "-rf", os.path.join(temp_base_dir, "*")], check=True
        )  # Clear previous contents

    print("\n--- Starting Valkey Standalone Server ---")
    # Start a single Valkey server in standalone mode
    process, client, node_info = start_valkey_server(
        start_port,
        conf_path,
        os.path.join(temp_base_dir, f"node_data_{start_port}"),
        os.path.join(temp_base_dir, f"node_log_{start_port}.log"),
    )
    if not process:
        print("Failed to start Valkey server. Aborting benchmark.", file=sys.stderr)
        return None

    try:
        populate_data_standalone(node_info, num_keys, key_value_size, temp_base_dir)

        num_keys_initial = get_db_key_count(start_port)
        assert num_keys == num_keys_initial, (
            f"num_keys does not match the actual number of keys on the db num_keys: {num_keys}, num_keys_db: {num_keys_initial}"
        )
        print(f"\n--- Keys before save = {num_keys_initial} ---")
        # Run BGSAVE on the single node using the already connected client
        bgsave_results = run_single_node_bgsave(client, temp_base_dir, DEFAULT_LOG_FILE)

        print("\n--- BGSAVE operation initiated and monitored. ---")

        # Restart the ValKey server and wait for it to load up the RDB file
        process, client, post_reload_verification_ok = test_rdb_reload_and_verify(
            process, client, start_port, conf_path, key_value_size, temp_base_dir
        )
        if (post_reload_verification_ok):
            num_keys_loaded = get_db_key_count(start_port)
            print(f"\n--- Keys before loaded from RDB = {num_keys_loaded} ---")
            if (num_keys_initial != num_keys_loaded):
                post_reload_verification_ok = False

        aggregated_results = {
            "keys": num_keys,
            "value_size": key_value_size,
            "num_nodes": 1,
            "data_dir": temp_base_dir,
            "post_reload_data_verified": post_reload_verification_ok,  # New result from the function
            "bgsave_status": bgsave_results["bgsave_status"],
            "bg_save_duration": bgsave_results["client_bgsave_duration"]
        }
        return aggregated_results

    finally:
        # Stop the single server process
        stop_valkey_server(process, client, start_port)


def main():
    parser = argparse.ArgumentParser(
        description="Valkey RDB Persistence Benchmark Tool for Standalone"
    )
    parser.add_argument(
        "--start-port",
        type=int,
        default=DEFAULT_START_PORT,
        help=f"Starting port for Valkey cluster nodes (default: {DEFAULT_START_PORT})",
    )
    parser.add_argument(
        "--num-keys",
        type=int,
        default=DEFAULT_NUM_KEYS,
        help=f"Number of keys to populate (default: {DEFAULT_NUM_KEYS})",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=DEFAULT_KEY_SIZE,
        help=f"Size of the value in bytes for populated keys (default: {DEFAULT_KEY_SIZE})",
    )
    parser.add_argument(
        "--conf",
        type=str,
        default=TEST_CONF_TEMPLATE,
        help=f"Path to the Valkey server configuration file template (default: {TEST_CONF_TEMPLATE}). This config should NOT contain cluster-enabled yes, as it's added by the script.",
    )
    parser.add_argument(
        "--temp-dir",
        type=str,
        default=os.path.join(os.getcwd(), f"{DEFAULT_TEMP_SUBDIR}_{time.time_ns()}"),
        help="Base directory for temporary data and logs for the cluster",
    )

    args = parser.parse_args()

    print("--- Starting RDB Persistence Benchmark for Standalone Valkey ---")
    results = run_bgsave_benchmark_standalone(
        args.start_port, args.conf, args.num_keys, args.value_size, args.temp_dir
    )

    if results:
        print("\n--- Benchmark Results ---")
        for k, v in results.items():
            print(f"{k}: {v}")
    else:
        print("\nBenchmark failed to complete.")


if __name__ == "__main__":
    main()
