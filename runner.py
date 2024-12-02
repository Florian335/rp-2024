import subprocess
import time

files_to_run = ["json-queries.py","pandas-queries.py","post-queries.py"]

iterations = 100

for file in files_to_run:
    print(f"Running {file} {iterations} times...")
    for i in range(iterations):
        print(f"Iteration {i + 1} for {file}")
        time.sleep(5)
        subprocess.run(["python", file])
