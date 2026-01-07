# RUSH - Resource-aware Query Scheduling across Heterogeneous Cloud Compute Services

RUSH 是一个云查询调度系统，智能地将查询路由到不同的云服务（EC2/Lambda/Athena），并进行服务内和服务间的负载均衡。

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                         RUSH Core                           │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │   Router    │→ │ Intra-Scheduler  │→ │ Inter-Scheduler│ │
│  │ (路由决策)   │  │  (服务内调度)     │  │  (服务间迁移)   │ │
│  └─────────────┘  └──────────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────────┘
           ↓                 ↓                    ↓
    ┌──────────┐      ┌──────────┐        ┌──────────┐
    │   EC2    │      │  Lambda  │        │  Athena  │
    │   (VM)   │      │  (FaaS)  │        │  (QaaS)  │
    └──────────┘      └──────────┘        └──────────┘
```

## 核心特性

### Router（路由器）

支持灵活的资源需求输入：
- **CSV 优先**: 使用预先收集的查询资源数据
- **ML 回退**: 对于 CSV 中不存在的查询，自动使用 ML 模型预测

Cost Model 基于 CloudGlide 的三阶段执行建模（I/O + CPU + Shuffle）。

### Intra-Scheduler（服务内调度）

在每个服务内部进行资源调度：
- 资源时间线管理（0.5秒/槽）
- 资源匹配评分
- 异步事件驱动 + 定时调度

### Inter-Scheduler（服务间调度）

跨服务的负载均衡和查询迁移：
- 监控队列等待时间
- 智能查询迁移
- 防止 ping-pong（冷却期、迁移次数限制）

## 快速开始

### 基本使用

```python
from src.router.router import Router
from src.models.query import Query, QueryType

# 初始化 Router (可选: 指定资源需求 CSV)
router = Router(resource_csv_path="data/query_resources.csv")

# 创建查询
query = Query(
    id="Q1",
    sql="SELECT * FROM users WHERE age > 25",
    query_type=QueryType.SELECT
)

# 路由查询
best_service = router.route_query(query)
print(f"Selected service: {best_service.value}")
```

### 资源需求 CSV 格式

创建 `data/query_resources.csv`:

```csv
query_id,cpu_time,data_scanned,scale_factor
Q1,1.196,38890000,50
Q2,0.176,10000000,25
Q3,0.654,38940000,75
```

**字段说明:**
- `query_id`: 查询 ID
- `cpu_time`: CPU 时间（秒）
- `data_scanned`: 扫描数据量（字节）
- `scale_factor`: 查询复杂度（1-100，影响 Shuffle 数据量）

## Cost Model

### EC2 (VM)
```
time = sum(pipeline_durations)
cost = time × (cost_per_hour / 3600) × max_resource_utilization
```

### Lambda (FaaS集群: 100×4GB)
```
time = cpu_time / cluster_cpu + io_time / cluster_io + shuffle_time
cost = gb_seconds × $0.0000002 + s3_access_cost
```

### Athena (QaaS)
```
time = athena_predictor.predict(pipeline_data, data_scanned)
cost = data_scanned_TB × $5.0
```

## 项目结构

```
RUSH/
├── src/
│   ├── router/
│   │   ├── router.py                  # 主路由器
│   │   ├── resource_provider.py       # 资源需求提供者
│   │   ├── detailed_cost_model.py     # 详细成本模型
│   │   └── ...
│   ├── scheduler/
│   │   ├── intra_scheduler.py         # 服务内调度
│   │   └── inter_scheduler.py         # 服务间调度
│   ├── executors/                     # 云服务执行器
│   ├── models/                        # 数据模型
│   └── config/                        # 配置参数
└── data/
    └── query_resources.csv            # 资源需求数据
```

## 配置参数

关键参数在 `src/config/parameters.py`:

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `COST_PERFORMANCE_TRADEOFF` | 1.0 | 成本-性能权衡 |
| `EC2_LOAD_BALANCE_FACTOR` | 0.05 | EC2 负载均衡系数 |
| `LAMBDA_LOAD_BALANCE_FACTOR` | 0.1 | Lambda 负载均衡系数 |
| `ATHENA_LOAD_BALANCE_FACTOR` | 0.15 | Athena 负载均衡系数 |

## 路由策略

评分公式:
```
score = time × cost^α × (1 + β × queue_size)
```

选择评分最低的服务。

## 依赖

- Python 3.9+
- DuckDB (查询解析)
- XGBoost (ML 预测)
- boto3 (AWS 服务)
- Flask (Web 服务)

完整依赖见 [requirements.txt](requirements.txt)
