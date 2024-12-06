# runner.py

import threading
import psutil
import time
import importlib
import json
import datetime
import os
import gc

def monitor_resources(stop_event, data_list, script_name):
    process = psutil.Process()
    # Initialize CPU measurement
    process.cpu_percent(interval=None)
    # Allow a small delay to initialize
    time.sleep(0.1)
    while not stop_event.is_set():
        cpu_percent = process.cpu_percent(interval=0.5)
        mem_percent = process.memory_percent()
        timestamp = datetime.datetime.now().isoformat()
        data = {
            'timestamp': timestamp,
            'script': script_name,
            'cpu_percent': cpu_percent,
            'memory_percent': mem_percent
        }
        data_list.append(data)

def run_scripts():
    scripts = [
        ('json_queries', 'json_queries.py'),
        ('pandas_queries', 'pandas_queries.py'),
        ('post_queries', 'post_queries.py')
    ]
    for module_name, script_filename in scripts:
        try:
            # Import and reload the module
            module = importlib.import_module(module_name)
            importlib.reload(module)

            # Prepare for resource monitoring
            stop_event = threading.Event()
            resource_data = []

            # Start resource monitoring thread
            monitor_thread = threading.Thread(
                target=monitor_resources,
                args=(stop_event, resource_data, script_filename)
            )
            monitor_thread.start()

            # Run the script's main function
            start_time = time.time()
            module.main()
            end_time = time.time()

            # Calculate total execution time
            total_execution_time = end_time - start_time

            # Stop the resource monitoring thread
            stop_event.set()
            monitor_thread.join()

            # Append total execution time to resource data
            summary_data = {
                'timestamp': datetime.datetime.now().isoformat(),
                'script': script_filename,
                'total_execution_time': total_execution_time
            }
            resource_data.append(summary_data)

            # Append resource data to JSON file
            output_file = 'resource_usage.json'
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            else:
                existing_data = []

            existing_data.extend(resource_data)

            with open(output_file, 'w') as f:
                json.dump(existing_data, f, indent=4)

            print(f"{script_filename} ran successfully.")
            print(f"Execution time: {total_execution_time:.4f}s\n")

            # Force garbage collection
            gc.collect()

        except Exception as e:
            print(f"An error occurred while running {script_filename}: {e}\n")

if __name__ == "__main__":
    for i in range(100):
        print(f"--- Iteration {i+1} ---")
        run_scripts()
