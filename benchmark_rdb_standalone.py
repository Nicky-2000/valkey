import subprocess
import time
import os
import sys
import valkey # Changed from import redis
import argparse
import re

# --- Configuration Constants ---
VALKEY_SERVER_PATH = "./src/valkey-server"
VALKEY_CLI_PATH = "./src/valkey-cli" # Not strictly needed if using valkey-py for commands
TEST_CONF_TEMPLATE = "testconfs/valkey7000.conf" # Your base config file
TEMP_DIR_PREFIX = "/tmp/valkey-benchmark-"
DEFAULT_PORT = 7000
DEFAULT_DB_FILE = "dump.rdb" # Default RDB filename Valkey uses
DEFAULT_LOG_FILE = "valkey.log" # Default log filename

# --- Helper Functions for Server Management ---

def start_valkey_server(port: int, conf_path, data_dir, log_file_path):
    """Starts a Valkey server instance in the background."""
    print(f"Starting Valkey server on port {port}...")

    # Ensure the data directory exists and is clean
    if os.path.exists(data_dir):
        subprocess.run(["rm", "-rf", os.path.join(data_dir, "*")], check=True)
    os.makedirs(data_dir, exist_ok=True)

    # Command to start Valkey server
    # We pass --dir and --dbfilename explicitly to control where RDB is saved
    # --loglevel and --logfile are crucial for capturing server-side timings
    command = [
        VALKEY_SERVER_PATH,
        conf_path,
        "--port", str(port),
        "--dir", data_dir,
        "--dbfilename", DEFAULT_DB_FILE,
        "--loglevel", "notice", # Ensure 'notice' level is enabled for your custom logs
        "--logfile", log_file_path,
        # Add --cluster-enabled yes --cluster-config-file nodes.conf if this is a cluster node setup
    ]

    try:
        # Popen runs the command in a new process, allowing the script to continue
        # preexec_fn=os.setsid detaches the child process from the current session,
        # making it a session leader. This helps prevent it from being killed if
        # the parent shell script exits prematurely (e.g., if you press Ctrl+C).
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)
        print(f"Valkey server started with PID: {process.pid} on port {port}. Log: {log_file_path}")
        time.sleep(1) # Give server a moment to start
        
        # Verify server is reachable
        vk_client = valkey.StrictValkey(host='127.0.0.1', port=port, decode_responses=True)
        vk_client.ping() # Will raise an exception if not reachable
        print(f"Server on port {port} is reachable.")
        return process, vk_client
    except Exception as e:
        print(f"Error starting Valkey server on port {port}: {e}")
        return None, None

def stop_valkey_server(process: subprocess.Popen, client: valkey.StrictRedis, port): # Changed type hint
    """Stops a Valkey server instance."""
    if not process:
        return

    print(f"Stopping Valkey server on port {port} (PID: {process.pid})...")
    try:
        # Try to shut down gracefully using Valkey's SHUTDOWN command
        client.shutdown(save=False) # We might want to control SAVE separately
        # Give it a moment to shut down
        time.sleep(1)
    except valkey.exceptions.ConnectionError: # Changed exception type
        print(f"Server on port {port} already disconnected (graceful shutdown).")
    except Exception as e:
        print(f"Error during graceful shutdown for port {port}: {e}")

    # Ensure the process is truly terminated
    if process.poll() is None: # Check if process is still running
        print(f"Valkey server on port {port} still running, killing forcefully...")
        # Use os.killpg to kill the process group, ensuring all children are killed
        os.killpg(os.getpgid(process.pid), 9) # SIGKILL
    
    process.wait() # Wait for the process to fully terminate
    print(f"Valkey server on port {port} stopped.")

# --- Data Population ---
def populate_data(client: valkey.StrictValkey, num_keys, key_value_size): # Changed type hint
    """Populates the Valkey server with string keys."""
    print(f"Populating {num_keys} keys with {key_value_size} bytes each...")
    # pipe = client.pipeline() # Use pipeline for efficiency
    for i in range(num_keys):
        key = f"key:{i}"
        value = "a" * key_value_size
        ret = client.set(key, value)
        if not ret: 
            print(f"Error setting key: {key}")
    #     if (i + 1) % 10000 == 0:
    #         pipe.execute()
    #         print(f"  {i+1}/{num_keys} keys populated.", end='\r')
    # pipe.execute() # Execute any remaining commands
    print(f"  {num_keys}/{num_keys} keys populated.")
    print("Data population complete.")

# --- Benchmarking Logic ---
def run_save_benchmark(port, conf_path, num_keys, key_value_size, temp_base_dir):
    """
    Runs a benchmark for the SAVE operation, including client-side timing
    and parsing server-side logs for internal timings.
    """
    data_dir = os.path.join(temp_base_dir, f"data-{port}")
    log_file_path = os.path.join(temp_base_dir, f"log-{port}-{DEFAULT_LOG_FILE}")
    
    # Clean up logs from previous runs in this temp dir
    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    server_process, client = start_valkey_server(port, conf_path, data_dir, log_file_path)
    if not server_process:
        print("Failed to start server. Aborting benchmark.")
        return None

    try:
        populate_data(client, num_keys, key_value_size)

        print("\n--- Running SAVE Benchmark ---")
        client_save_start_time = time.perf_counter()
        
        # Trigger SAVE command
        client.save() # This is a blocking call from the client's perspective

        client_save_end_time = time.perf_counter()
        client_save_duration = client_save_end_time - client_save_start_time
        print(f"Client-side SAVE command completed in: {client_save_duration:.4f} seconds.")

        # --- Parse server log for internal timings ---
        server_start_us = None
        server_end_us = None
        server_log_duration_us = None

        print(f"Parsing server log file: {log_file_path}")
        try:
            with open(log_file_path, 'r') as f:
                log_content = f.read()

            # Regex to find your custom log messages
            # Adjust regex based on your exact serverLog format
            start_match = re.search(r'RDB Save started at (\d+)us', log_content)
            end_match = re.search(r'RDB Save finished at (\d+)us\. Total duration: (\d+)us', log_content)

            if start_match:
                server_start_us = int(start_match.group(1))
            if end_match:
                server_end_us = int(end_match.group(1))
                server_log_duration_us = int(end_match.group(2))

            if server_start_us and server_end_us:
                calculated_duration_us = server_end_us - server_start_us
                print(f"Server-side (calculated from log) RDB Save duration: {calculated_duration_us / 1000000:.4f} seconds.")
                print(f"Server-side (reported in log) RDB Save duration: {server_log_duration_us / 1000000:.4f} seconds.")
            else:
                print("Could not find internal RDB Save timing messages in server log.")

        except FileNotFoundError:
            print(f"Server log file not found at {log_file_path}. Check --dir and --logfile settings.")
        except Exception as e:
            print(f"Error parsing log file: {e}")

        return {
            "client_save_duration": client_save_duration,
            "server_log_duration": server_log_duration_us / 1000000 if server_log_duration_us else None,
            "keys": num_keys,
            "value_size": key_value_size,
            "port": port,
            "data_dir": data_dir,
            "log_file": log_file_path,
        }

    finally:
        stop_valkey_server(server_process, client, port)


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="Valkey RDB Persistence Benchmark Tool")
    parser.add_argument("--port",
                        type=int,
                        default=DEFAULT_PORT,
                        help=f"Port for the Valkey server (default: {DEFAULT_PORT})")
    
    parser.add_argument("--keys",
                        type=int,
                        default=100000,
                        help="Number of keys to populate (default: 100000)")
    
    parser.add_argument("--value-size",
                        type=int,
                        default=100,
                        help="Size of the value in bytes for populated keys (default: 100)")
    
    parser.add_argument("--conf",
                        type=str,
                        default=TEST_CONF_TEMPLATE,
                        help=f"Path to the Valkey server configuration file (default: {TEST_CONF_TEMPLATE})")
    
    parser.add_argument("--temp-dir",
                        type=str,
                        default=f"{TEMP_DIR_PREFIX}{time.time_ns()}",
                        help="Base directory for temporary data and logs")

    args = parser.parse_args()

    # Create a unique temporary directory for this run
    if not os.path.exists(args.temp_dir):
        os.makedirs(args.temp_dir)
        print(f"Created temporary directory: {args.temp_dir}")
    else:
        print(f"Using existing temporary directory: {args.temp_dir}")

    print("--- Starting RDB Persistence Benchmark ---")
    results = run_save_benchmark(
        args.port,
        args.conf,
        args.keys,
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
    # Uncomment the following line if you want to automatically clean up the temp directory
    # subprocess.run(["rm", "-rf", args.temp_dir], check=True)
    print("Cleanup complete. Review log files in temp directory if not removed.")

if __name__ == "__main__":
    main()