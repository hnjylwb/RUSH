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
- `--resources`: CSV file with query resource requirements

### 3. Submit Queries

List available queries:
```bash
python run_client.py tpch
python run_client.py ssb
python run_client.py clickbench
```

Submit specific queries:
```bash
python run_client.py tpch 1 2 3
python run_client.py ssb 1.1 2.1 3.1
```

Optional arguments:
- `--server`: Server URL (default: http://localhost:8080)
- `--client-id`: Client identifier

## Directory Structure

```
├── run_server.py          # Server startup script
├── run_client.py          # Client startup script
├── ease/                  # Core library
│   ├── server.py          # Server implementation
│   ├── client.py          # Client implementation
│   ├── core/              # Data models
│   ├── executors/         # Executors (VM/FaaS/QaaS)
│   ├── router/            # Router with cost models
│   ├── scheduler/         # Schedulers
│   └── config/            # Configuration management
├── config/                # Configuration files
└── queries/               # Query files
    ├── tpch/              # TPC-H queries
    ├── ssb/               # SSB queries
    └── clickbench/        # ClickBench queries
```
