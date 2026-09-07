# CHIA Log Analyzer

A small hands-on CHIA + Ray project that demonstrates how a distributed workflow is defined, scheduled, and executed across logical workers — including an optional second physical server.

The log-analysis logic is intentionally simple. The real purpose is to make the CHIA execution model easy to understand.

## Core Mental Model

- **Cluster** = where work can run.
- **Worker** = an execution resource in the cluster.
- **CHIA node** = a schedulable computation defined with `@ChiaFunction`.
- **Task** = one execution of a CHIA node.
- **Job** = one run of the application.
- **Driver** = the Python process running `main.py`.
- **Ray** = the scheduler/runtime underneath CHIA.
- **ObjectRef** = a reference to a remote result.
- **`.chia_remote()`** = submit a remote task.
- **`get()`** = wait for and retrieve a remote result.

> This project is a **CHIA distributed workflow / pipeline**, not yet a true cyclic CHIA loop.

---

## 1. Project Idea

The application reads several log files, analyzes each one, and combines the individual results into one report.

```text
sample_logs/
├── app1.log
├── app2.log
└── app3.log
       |
       v
analyze_file() tasks
       |
       v
individual analysis results
       |
       v
aggregate_results()
       |
       v
final report
```

Each `analyze_file()` task is a CHIA node execution. Ray decides where it executes by matching the task's requested resources to resources advertised by workers.

---

## 2. Repository Structure

```text
chia-log-analyzer/
├── cluster.yaml
├── main.py
├── analyzer.py
├── remote_test.py          # optional multi-server test
└── sample_logs/
    ├── app1.log
    ├── app2.log
    └── app3.log
```

### `cluster.yaml`

Defines the execution environment:

- Ray head
- logical workers
- custom worker resources
- SSH configuration
- environment setup
- optional remote physical machines

Think of it as:

```text
cluster.yaml = WHERE and HOW computation may run
```

### `analyzer.py`

Contains deterministic Python logic. It counts:

- total lines
- `INFO`
- `WARNING`
- `ERROR`
- IP addresses
- repeated IP addresses

It does not need to know anything about CHIA or Ray.

```text
analyzer.py = HOW one log should be analyzed
```

### `main.py`

The driver/orchestrator. It:

1. discovers `.log` files
2. submits one remote `analyze_file()` task per file
3. stores the returned `ObjectRef`s
4. waits for the results with `get()`
5. submits `aggregate_results()`
6. retrieves and prints the final report

```text
main.py = WHAT work should happen and in what order
```

### `sample_logs/`

The input workload. These are just normal text files.

---

## 3. Cluster, Job, Node, Task, Worker, Driver, and Ray

```text
CLUSTER
|
├── Ray Head
├── Worker A
├── Worker B
└── optional Worker C on another server

JOB
|
└── python main.py
     |
     ├── analyze_file task
     ├── analyze_file task
     ├── analyze_file task
     └── aggregate_results task
```

The clean distinction is:

```text
cluster = where work CAN run
job     = one run of the application
node    = a CHIA computation definition
task    = one execution of a node
worker  = where the task actually executes
driver  = process controlling the workflow
Ray     = scheduler choosing a compatible worker
```

---

## 4. How Ray Knows What to Do

Workers advertise resources they **have**.

```yaml
analysis_worker:
    resources: {"analysis": 1}
```

This means:

```text
analysis_worker HAS analysis=1
```

A CHIA node declares resources it **needs**.

```python
@ChiaFunction(resources={"analysis": 1})
def analyze_file(...):
    ...
```

This means:

```text
analyze_file task NEEDS analysis=1
```

Ray matches them:

```text
task NEEDS analysis=1
          |
          v
         Ray
          |
          v
worker HAS analysis=1
          |
          v
execute task there
```

Ray does not need to understand the semantic meaning of `analysis`. It treats it as a custom schedulable resource label.

---

## 5. CHIA Node vs Task vs Worker

### CHIA Node

A node is a computation definition:

```python
@ChiaFunction(resources={"analysis": 1})
def analyze_file(path_str: str):
    ...
```

### Task

One execution of that node:

```python
ref = analyze_file.chia_remote("sample_logs/app1.log")
```

Calling it three times creates three separate tasks.

### Worker

The worker is the execution resource/process that actually runs the task.

```text
CHIA node definition
       |
       v
.chia_remote()
       |
       v
task request
       |
       v
Ray scheduler
       |
       v
matching worker
       |
       v
Python executes
```

---

## 6. ObjectRefs and `get()`

Remote execution is asynchronous.

```python
ref = analyze_file.chia_remote(...)
```

returns an `ObjectRef`, not necessarily the final value immediately.

Think of an ObjectRef as:

```text
ObjectRef = ticket/reference to a result that may still be running
```

Then:

```python
result = get(ref)
```

means:

```text
wait for the remote result and return the real value
```

For multiple tasks:

```python
analysis_results = get(analysis_refs)
```

waits for all referenced results.

---

## 7. Prerequisites

The demo environment used:

- Ubuntu 24.04
- Python 3.10.19
- CHIA / `chialoops` 1.0.1
- Ray 2.54.0
- Git
- OpenSSH
- `uv`

Docker is **not required** for this project.

All machines joining one Ray cluster should use compatible Python, Ray, and CHIA environments.

---

## 8. Install CHIA

```bash
cd ~
git clone https://github.com/ucb-bar/chia.git
cd chia

uv venv --python 3.10.19 .venv
source .venv/bin/activate
uv pip install -e .
```

Verify:

```bash
python --version
ray --version
chia --help
```

For the demo, expected versions were:

```text
Python 3.10.19
Ray 2.54.0
CHIA CLI available
```

---

## 9. SSH Setup

CHIA uses SSH to manage hosts and start Ray processes.

Generate a key if needed:

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
```

For a local/self-hosted demo:

```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Verify SSH:

```bash
ssh -i ~/.ssh/id_ed25519 "$USER"@YOUR_SERVER_IP
```

For multiple machines, the head machine must be able to SSH into each worker machine without manual password entry.

---

## 10. Single-Server Cluster Example

```yaml
provider:
    head_ip: "${THIS_MACHINE}"

auth:
    ssh_user: "${USER}"
    ssh_private_key: ~/.ssh/id_ed25519

available_node_types:
    analysis_worker:
        resources: {"analysis": 1}
        num_workers: 1
        compatible_ips: ["${THIS_MACHINE}"]
        worker_env_commands:
            - "source /home/adam/chia/.venv/bin/activate"

    aggregation_worker:
        resources: {"aggregation": 1}
        num_workers: 1
        compatible_ips: ["${THIS_MACHINE}"]
        worker_env_commands:
            - "source /home/adam/chia/.venv/bin/activate"

head_env_commands:
    - "source /home/adam/chia/.venv/bin/activate"

head_start_ray_commands:
    - "ray stop"
    - "ray start --head --port=6379 --include-dashboard=True --dashboard-agent-listen-port=0"

worker_start_ray_commands:
    - "ray stop"
    - "ray start --address=$RAY_HEAD_IP:6379 --dashboard-agent-listen-port=0"
```

Set variables:

```bash
export THIS_MACHINE=YOUR_HEAD_SERVER_IP
export USER=YOUR_USERNAME
```

---

## 11. Start and Inspect the Cluster

Start:

```bash
chia up cluster.yaml
```

Status:

```bash
chia status --chia-cluster cluster.yaml
```

List nodes:

```bash
chia list --chia-cluster cluster.yaml nodes
```

You should see workers advertising:

```text
analysis: 1
aggregation: 1
```

---

## 12. Run the Log Analyzer

From the project directory:

```bash
chia job submit --working-dir . -- python main.py
```

Breakdown:

```text
chia            -> use CHIA CLI
job submit       -> submit one application run
--working-dir .  -> package current project directory
--               -> end CHIA CLI options
python main.py   -> run main.py as the job driver
```

Ray packages the working directory so the job can access:

```text
main.py
analyzer.py
sample_logs/
```

---

## 13. Expected Workflow

```text
main.py driver
    |
    v
find app1.log / app2.log / app3.log
    |
    +--------------------------------+
    |              |                 |
    v              v                 v
analyze_file   analyze_file      analyze_file
(app1)         (app2)            (app3)
    |              |                 |
    +--------------+-----------------+
                   |
                   v
            ObjectRefs
                   |
                   v
             get(refs)
                   |
                   v
         individual results
                   |
                   v
         aggregate_results()
                   |
                   v
             final report
```

Expected totals for the sample workload:

```text
files_processed = 3
total_lines     = 17
errors          = 4
warnings        = 4
info            = 9
```

---

## 14. Example `analyzer.py`

```python
from collections import Counter
import re


def analyze_log_text(text: str) -> dict:
    lines = text.splitlines()

    error_count = 0
    warning_count = 0
    info_count = 0

    ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    ips = []

    for line in lines:
        upper_line = line.upper()

        if "ERROR" in upper_line:
            error_count += 1
        if "WARNING" in upper_line:
            warning_count += 1
        if "INFO" in upper_line:
            info_count += 1

        ips.extend(re.findall(ip_pattern, line))

    ip_counts = Counter(ips)

    return {
        "total_lines": len(lines),
        "errors": error_count,
        "warnings": warning_count,
        "info": info_count,
        "ip_addresses": dict(ip_counts),
    }
```

---

## 15. Example `main.py`

```python
from pathlib import Path
from chia.base.ChiaFunction import ChiaFunction, get
from analyzer import analyze_log_text


@ChiaFunction(resources={"analysis": 1})
def analyze_file(path_str: str) -> dict:
    path = Path(path_str)
    text = path.read_text()
    result = analyze_log_text(text)

    return {
        "file": path.name,
        "result": result,
    }


@ChiaFunction(resources={"aggregation": 1})
def aggregate_results(results: list[dict]) -> dict:
    total_files = len(results)
    total_lines = 0
    total_errors = 0
    total_warnings = 0
    total_info = 0

    for item in results:
        data = item["result"]
        total_lines += data["total_lines"]
        total_errors += data["errors"]
        total_warnings += data["warnings"]
        total_info += data["info"]

    return {
        "files_processed": total_files,
        "total_lines": total_lines,
        "errors": total_errors,
        "warnings": total_warnings,
        "info": total_info,
    }


def main():
    log_files = sorted(Path("sample_logs").glob("*.log"))
    print(f"Found {len(log_files)} log files.")

    analysis_refs = []

    for path in log_files:
        ref = analyze_file.chia_remote(str(path))
        analysis_refs.append(ref)
        print(f"Submitted: {path.name}")
        print(f"ObjectRef: {ref}")

    analysis_results = get(analysis_refs)

    print("\nIndividual results:")
    for item in analysis_results:
        print(item)

    aggregate_ref = aggregate_results.chia_remote(analysis_results)

    print("\nAggregation task submitted.")
    print(f"ObjectRef: {aggregate_ref}")

    final_report = get(aggregate_ref)

    print("\nFinal report:")
    print(final_report)


if __name__ == "__main__":
    main()
```

---

## 16. Optional Second Physical Server

This experiment demonstrates that the project and driver can remain on the head server while Ray executes selected CHIA tasks on another machine.

Demo topology used during development:

```text
HEAD SERVER
173.212.217.138
user: adam
|
├── Ray Head
├── analysis_worker
└── aggregation_worker

REMOTE SERVER
169.58.139.244
user: ysf
|
└── remote_test_worker
    remote_test=1
```

> The IP addresses and usernames above are demo values. Replace them with your own environment.

### Multi-server `cluster.yaml`

```yaml
cluster_name: chia-log-analyzer

provider:
    head_ip: "${THIS_MACHINE}"

auth:
    ssh_user: "${USER}"
    ssh_private_key: ~/.ssh/id_ed25519
    overrides:
        169.58.139.244:
            ssh_user: ysf
            ssh_private_key: ~/.ssh/id_ed25519

available_node_types:
    analysis_worker:
        resources: {"analysis": 1}
        num_workers: 1
        compatible_ips:
            - "${THIS_MACHINE}"
        worker_env_commands:
            - "source /home/adam/chia/.venv/bin/activate"

    aggregation_worker:
        resources: {"aggregation": 1}
        num_workers: 1
        compatible_ips:
            - "${THIS_MACHINE}"
        worker_env_commands:
            - "source /home/adam/chia/.venv/bin/activate"

    remote_test_worker:
        resources: {"remote_test": 1}
        num_workers: 1
        compatible_ips:
            - "169.58.139.244"
        worker_env_commands:
            - "source /home/ysf/chia/.venv/bin/activate"

head_env_commands:
    - "source /home/adam/chia/.venv/bin/activate"

head_start_ray_commands:
    - "ray stop"
    - "ray start --head --port=6379 --include-dashboard=True --dashboard-agent-listen-port=0"

worker_start_ray_commands:
    - "ray stop"
    - "ray start --address=$RAY_HEAD_IP:6379 --dashboard-agent-listen-port=0"
```

---

## 17. Remote Worker Test

Example `remote_test.py`:

```python
import os
import socket
from chia.base.ChiaFunction import ChiaFunction, get


@ChiaFunction(resources={"remote_test": 1})
def where_am_i():
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "user": os.getenv("USER"),
    }


def main():
    print("Driver hostname:", socket.gethostname())
    print("Driver PID:", os.getpid())

    ref = where_am_i.chia_remote()

    print("Remote task submitted.")
    print("ObjectRef:", ref)

    result = get(ref)

    print("\nRemote worker result:")
    print(result)


if __name__ == "__main__":
    main()
```

Run:

```bash
chia job submit --working-dir . -- python remote_test.py
```

If only Server 2 advertises `remote_test=1`, Ray must schedule the task there.

```text
Server 1
Driver runs here
    |
    v
where_am_i.chia_remote()
    |
    v
Ray scheduler
    |
    | needs remote_test=1
    v
Server 2
remote_test_worker
    |
    v
where_am_i() executes here
```

This proves that code can be submitted from one server while computation executes using another server's CPU/RAM.

---

## 18. Physical Machine vs Ray Node

One physical machine can host multiple logical Ray nodes.

```text
one physical VPS
|
├── Ray Head
├── analysis worker
└── aggregation worker
```

All may share one IP while appearing as separate Ray nodes.

```text
physical server != Ray logical node
```

Be careful with CPU oversubscription: multiple logical Ray nodes on one physical server can each advertise CPU capacity, causing Ray's schedulable CPU count to exceed the machine's physical vCPU count.

---

## 19. Is This a CHIA Loop?

Not yet in the strict cyclic sense.

Current workflow:

```text
analyze
   |
   v
aggregate
   |
   v
finish
```

A true loop needs feedback:

```text
analyze
   |
   v
evaluate
   |
   v
stop?
├── yes -> finish
└── no  -> analyze again
```

So this project is best described as:

> **A CHIA distributed workflow demonstrating resource-aware task scheduling and multi-worker execution.**

---

## 20. Useful Commands

Start cluster:

```bash
chia up cluster.yaml
```

Stop cluster:

```bash
chia down cluster.yaml
```

Cluster status:

```bash
chia status --chia-cluster cluster.yaml
```

List nodes:

```bash
chia list --chia-cluster cluster.yaml nodes
```

Submit log analyzer:

```bash
chia job submit --working-dir . -- python main.py
```

Submit remote worker test:

```bash
chia job submit --working-dir . -- python remote_test.py
```

Check Ray:

```bash
ray status
```

Stop stale Ray processes:

```bash
ray stop
```

---

## 21. Troubleshooting

### `${THIS_MACHINE}` appears literally

```bash
export THIS_MACHINE=YOUR_HEAD_IP
```

Verify:

```bash
echo "$THIS_MACHINE"
```

### SSH asks for a password

Configure public-key authentication:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub user@REMOTE_IP
```

Verify non-interactive login:

```bash
ssh -o BatchMode=yes -i ~/.ssh/id_ed25519 user@REMOTE_IP \
'echo "SSH OK"; whoami; hostname'
```

### Ray cannot join the cluster

Check compatible versions on all machines:

```bash
python --version
ray --version
```

### Old cluster still running

```bash
ray stop
```

### Worker cannot import CHIA

Verify the correct venv is activated in `worker_env_commands`.

### Task remains pending

Check resources:

```bash
chia status
```

If a task needs `analysis=1` but no worker advertises `analysis=1`, Ray cannot schedule it.

---

## 22. Why This Project Is Useful

Although the workload is simple, the project demonstrates the same core architecture used by larger CHIA systems:

```text
driver
   |
   v
CHIA node
   |
   v
task
   |
   v
Ray scheduler
   |
   v
specialized worker
   |
   v
result
```

The same pattern can later be extended to:

- benchmark workers
- simulation workers
- GPU workers
- gem5 workers
- FPGA workers
- verification workers
- evaluation nodes
- iterative optimization loops

---

## 23. Possible Next Steps

- multiple `analysis` resource slots for real concurrency
- timing comparison between serial and parallel execution
- persistent result storage
- retry/failure handling
- profiling
- Docker-based worker environments
- verification node
- cyclic feedback condition
- automatic repeated analysis based on results

That would evolve the project from a distributed pipeline toward a true CHIA loop.

---

## 24. One-Sentence Summary

> The CHIA Log Analyzer keeps workflow logic in Python, defines specialized worker capabilities in `cluster.yaml`, submits remote node executions with `.chia_remote()`, lets Ray match task requirements to worker resources, and retrieves distributed results using `ObjectRef`s and `get()`.
