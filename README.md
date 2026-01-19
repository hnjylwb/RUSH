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

### 3. Submit Queries

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

## Deploy Lambda Function

Deploy the query executor to AWS Lambda for FaaS support:

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

## Directory Structure

```
├── run_server.py          # Server startup script
├── run_client.py          # Client script (one-time & interactive modes)
├── ease/                  # Core library
│   ├── server.py          # Server implementation
│   ├── client.py          # Client implementation
│   ├── core/              # Data models
│   ├── executors/         # Executors (VM/FaaS/QaaS)
│   ├── router/            # Router with cost models
│   ├── scheduler/         # Schedulers
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
