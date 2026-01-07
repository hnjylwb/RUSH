# Query Scheduling System

跨异构云服务的查询调度系统

## 架构

```
┌─────────────────────────────────────────┐
│            System                       │
│  ┌────────┐  ┌─────────────────────┐   │
│  │ Router │→ │ Intra-Scheduler     │   │
│  └────────┘  │ Inter-Scheduler     │   │
│              └─────────────────────┘   │
└─────────────────────────────────────────┘
         ↓             ↓            ↓
    ┌─────┐       ┌──────┐     ┌──────┐
    │ VM  │       │ FaaS │     │ QaaS │
    └─────┘       └──────┘     └──────┘
```

## 核心组件

### 1. Executors (执行器)
- **VM**: Virtual Machine (e.g., EC2)
- **FaaS**: Function as a Service (e.g., Lambda)
- **QaaS**: Query as a Service (e.g., Athena)

### 2. Router (路由器)
包含三个模型:
- **Resource Model**: 估算查询资源需求
- **Performance Model**: 估算执行时间
- **Cost Model**: 估算金钱成本

### 3. Schedulers (调度器)
- **Intra-Scheduler**: 服务内调度
- **Inter-Scheduler**: 跨服务迁移

## 快速开始

```python
from ease import System, Query, QueryType

# 初始化系统
system = System(
    config_dir="config",
    resource_csv="data/query_resources.csv"
)

# 创建查询
query = Query(
    query_id="Q1",
    sql="SELECT * FROM users WHERE age > 25",
    query_type=QueryType.OLTP
)

# 提交查询
await system.submit_query(query)
```

## 运行演示

```bash
python demo.py
```

## 目录结构

```
ease/                   # 主代码
├── core/              # 核心数据模型
├── executors/         # VM/FaaS/QaaS执行器
├── router/            # 路由器 + 模型
├── scheduler/         # 调度器
└── config/            # 配置管理

config/                # 配置文件
├── services.yaml
└── parameters.yaml

data/                  # 数据
└── query_resources.csv
```

## 路由策略

评分公式:
```
score = time × cost^α × (1 + β × queue_size)
```

选择评分最低的服务。

## 配置

在 `config/` 目录下修改:
- `services.yaml`: 服务配置
- `parameters.yaml`: 参数配置
