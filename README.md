# Query Scheduling System

Cross-cloud heterogeneous query scheduling system

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Server

```bash
python run_server.py
```

Optional arguments:
- `--host`: Server host address (default: 0.0.0.0)
- `--port`: Server port (default: 8080)
- `--config`: Configuration directory (default: config)

### 3. Deploy VM Workers (Optional)

If you want to use VM executors, deploy workers to your VM nodes.

For auto-registration with the scheduler (recommended for auto-scaling):

```bash
python run_vm_worker.py --scheduler http://localhost:8080
```

Or start without auto-registration (requires manual configuration in config/services.yaml):

```bash
python run_vm_worker.py --host 0.0.0.0 --port 8081
```

Optional arguments:
- `--host`: Host address to bind to (default: 0.0.0.0)
- `--port`: Port to listen on (default: 8081)
- `--data-dir`: Directory containing data files
- `--scheduler`: Scheduler URL for auto-registration (e.g., http://localhost:8080)

You can run multiple workers on different VMs or different ports. Workers will be automatically assigned to the first available VM executor by the scheduler.

### 4. Deploy Lambda Function (Optional)

If you want to use FaaS support, deploy the query executor to AWS Lambda:

```bash
cd lambda
python deploy.py
```

This will:
- Package the Lambda function with DuckDB dependencies
- Create IAM execution role with necessary permissions
- Deploy or update the function on AWS Lambda

Requirements:
- AWS credentials configured via `aws configure`
- boto3 installed: `pip install boto3`

### 5. Configure Services

Edit `config/services.yaml` to configure which services to use. By default, all three service types are enabled:

```yaml
enabled_services:
  - vm
  - faas
  - qaas
```

To disable a service type, remove it from the list or comment it out. For example, to use only VM and FaaS:

```yaml
enabled_services:
  - vm
  - faas
```

Also update the service endpoints in the same file to match your deployment.

Note: When using VM worker auto-registration, you don't need to configure VM endpoints in this file.

### 6. Submit Queries

The client supports two modes: one-time submission and interactive mode.

#### One-time mode (default)

List available queries:
```bash
python run_client.py tpch
python run_client.py ssb
```

Submit specific queries:
```bash
python run_client.py tpch 1 2 3
python run_client.py ssb 1.1 2.1 3.1 --resources resources/ssb
```

#### Interactive mode

Start interactive client (no benchmark argument or use `-i` flag):
```bash
python run_client.py
python run_client.py -i --resources resources
```

Available commands in interactive mode:
- `list <benchmark>` - List available queries
- `submit <benchmark> <q1> [q2 ...]` - Submit queries
- `status` - Get server status
- `tasks` - Show submitted tasks count
- `resources` - Show cached resources
- `clear` - Clear resource cache
- `help` - Show help
- `exit` - Exit client

Optional arguments:
- `--server`: Server URL (default: http://localhost:8080)
- `--client-id`: Client identifier
- `--resources`: Directory containing query resource JSON files
- `-i, --interactive`: Force interactive mode

## Directory Structure

```
├── run_server.py          # Server startup script
├── run_client.py          # Client script (one-time & interactive modes)
├── ease/                  # Core library
│   ├── server.py          # Server implementation
│   ├── client.py          # Client implementation
│   ├── cost_model.py      # Cost model for estimating time and cost
│   ├── core/              # Data models
│   ├── executors/         # Executors (VM/FaaS/QaaS)
│   ├── scheduler/         # Scheduler and Rescheduler
│   └── config/            # Configuration management
├── lambda/                # AWS Lambda function
│   ├── lambda_function.py # Query executor for Lambda
│   ├── deploy.py          # Deployment script
│   └── requirements.txt   # Lambda dependencies
├── config/                # Configuration files
└── queries/               # Query files
    ├── tpch/              # TPC-H queries
    ├── ssb/               # SSB queries
    └── clickbench/        # ClickBench queries
```
