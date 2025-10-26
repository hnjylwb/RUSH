"""
RUSH System Parameters Configuration

This file contains all tunable parameters for the scheduling system.
Modify these values to tune the scheduling behavior.
"""

class RUSHParameters:
    """All RUSH system parameters in one place"""
    
    # Cost-performance trade-off parameter
    COST_PERFORMANCE_TRADEOFF = 1.0  # Alpha in paper: weight for cost vs performance in service scoring
    
    # Migration threshold parameters for inter-scheduler
    WAIT_TIME_MULTIPLIER = 0.2      # Gamma in paper: multiplier for predicted latency in threshold calculation
    MIN_WAIT_THRESHOLD = 1.0        # Tau in paper: minimum wait time threshold (seconds)
    
    # Other scheduling weights
    AVAILABILITY_WEIGHT = 0.2       # Weight for service availability in scoring
    
    # Time-related parameters
    INTER_SCHEDULER_INTERVAL = 1.0  # Inter-scheduler rebalancing interval (seconds)
    
    # Migration control parameters
    MIGRATION_COOLDOWN_PERIOD = 5.0  # Seconds before a query can be migrated again
    MAX_MIGRATIONS_PER_QUERY = 3  # Maximum migrations allowed per query
    PING_PONG_PREVENTION_WINDOW = 30.0  # Seconds to check for ping-pong migrations
    
    # Load balancing thresholds
    LOAD_REBALANCE_THRESHOLD = 2    # Queue size difference threshold for rebalancing
    PROCESSING_CHECK_INTERVAL = 0.1  # Intra-scheduler processing interval (seconds)
    
    # Time-sliced scheduling parameters
    TIME_SLOT_DURATION = 0.5            # Duration of each time slot in seconds
    MAX_RESOURCE_CAPACITY = 1.0         # Maximum resource capacity (normalized to 1.0)
    RESOURCE_VECTOR_LENGTH = 10         # Length for resource matching vectors (padding length)
    
    # Service capacity parameters (resource units per second)
    SERVICE_CAPACITIES = {
        'EC2': {'cpu': 1.0, 'memory': 1.0, 'io': 1.0},  # Each resource dimension capacity
        'LAMBDA': 1000.0,    # Instance units per second
        'ATHENA': 30.0       # Query slots per second
    }
    
    # Router parameters
    ROUTER_PRIORITY_WEIGHT = 0.6    # Priority weight in router service selection
    ROUTER_LOAD_WEIGHT = 0.4        # Load weight in router service selection
    
    # Service-specific configuration parameters (only used ones)
    EC2_DEFAULT_COST_PER_HOUR = 1.536  # VM cost per hour in dollars
    EC2_LOAD_BALANCE_FACTOR = 0.05
    
    LAMBDA_DEFAULT_COST_PER_INVOCATION = 0.0000002
    LAMBDA_COST_PER_SECOND = 0.0000002  # Cost per GB-second
    LAMBDA_COST_PER_MS = 0.0000166667
    LAMBDA_LOAD_BALANCE_FACTOR = 0.1
    
    # S3 access costs for Lambda shuffle operations
    S3_READ_COST_PER_1000_REQUESTS = 0.0004   # $0.0004 per 1000 GET requests
    S3_WRITE_COST_PER_1000_REQUESTS = 0.005   # $0.005 per 1000 PUT requests
    
    ATHENA_DEFAULT_COST_PER_TB = 5.0
    ATHENA_LOAD_BALANCE_FACTOR = 0.15
    
    # Queue management
    MAX_QUEUE_SIZE = 1000
    
    # Demo and development settings
    DEMO_PROCESSING_DELAY = 10  # Seconds to let background schedulers process in demo
    SIMULATION_MODE = True  # Global setting: True for simulation, False for real execution


def calculate_migration_threshold(predicted_latency: float) -> float:
    """Calculate migration threshold using gamma * latency and tau parameters"""
    params = RUSHParameters()
    return max(
        params.WAIT_TIME_MULTIPLIER * predicted_latency,
        params.MIN_WAIT_THRESHOLD
    )
