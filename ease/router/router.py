"""
Router - 查询路由决策
"""

from typing import Dict, List, Optional
from ..core import Query, ServiceType
from .models import ResourceModel, PerformanceModel, CostModel


class RoutingDecision:
    """Routing decision result"""

    def __init__(self, selected_service: ServiceType,
                 estimates: Dict[str, Dict],
                 scores: Dict[str, float]):
        self.selected_service = selected_service
        self.estimates = estimates  # {service: {time, cost, ...}}
        self.scores = scores        # {service: score}


class Router:
    """
    Router

    Responsibilities:
    1. Estimate query resource requirements
    2. Estimate execution time/cost on each service
    3. Select the best service based on scoring function
    """

    def __init__(self, config: Dict = None,
                 service_configs: Dict[ServiceType, Dict] = None,
                 resource_csv: Optional[str] = None):
        """
        Initialize router

        Args:
            config: Router configuration
            service_configs: Configuration for each service type
            resource_csv: Path to CSV with query resource requirements
        """
        self.config = config or {}
        self.service_configs = service_configs or {}

        # Initialize models
        self.resource_model = ResourceModel(csv_path=resource_csv)
        self.performance_model = PerformanceModel(config=self.config.get('performance', {}))
        self.cost_model = CostModel(config=self.config.get('cost', {}))

        # Scoring parameters
        self.cost_weight = self.config.get('cost_weight', 1.0)        # α
        self.load_weights = self.config.get('load_weights', {         # β for each service
            ServiceType.VM: 0.05,
            ServiceType.FAAS: 0.1,
            ServiceType.QAAS: 0.15
        })

    def route(self, query: Query, queue_sizes: Optional[Dict[ServiceType, int]] = None) -> RoutingDecision:
        """
        Route query to best service

        Args:
            query: Query to route
            queue_sizes: Current queue sizes for each service

        Returns:
            RoutingDecision with selected service and estimates
        """
        if queue_sizes is None:
            queue_sizes = {}

        # Step 1: Estimate resource requirements
        resources = self.resource_model.estimate(
            query.query_id,
            query.sql,
            provided_resources=query.resource_requirements
        )
        query.resource_requirements = resources.to_dict()

        # Step 2: Estimate time and cost for each service
        estimates = {}
        scores = {}

        for service_type in [ServiceType.VM, ServiceType.FAAS, ServiceType.QAAS]:
            # Get service configuration
            service_config = self.service_configs.get(service_type, {})

            # Estimate performance
            if service_type == ServiceType.VM:
                perf = self.performance_model.estimate_vm(resources, service_config)
                cost = self.cost_model.estimate_vm(perf.execution_time, service_config, resources)
            elif service_type == ServiceType.FAAS:
                perf = self.performance_model.estimate_faas(resources, service_config)
                cost = self.cost_model.estimate_faas(perf.execution_time, service_config)
            elif service_type == ServiceType.QAAS:
                perf = self.performance_model.estimate_qaas(resources, service_config)
                cost = self.cost_model.estimate_qaas(resources, service_config)

            # Store estimates
            estimates[service_type] = {
                'time': perf.execution_time,
                'cost': cost,
                'breakdown': perf.breakdown
            }

            # Calculate score
            queue_size = queue_sizes.get(service_type, 0)
            load_weight = self.load_weights.get(service_type, 0.1)
            score = self._calculate_score(perf.execution_time, cost, queue_size, load_weight)
            scores[service_type] = score

        # Step 3: Select service with lowest score
        best_service = min(scores, key=scores.get)

        # Update query
        query.routing_decision = best_service.value
        query.estimated_time = estimates[best_service]['time']
        query.estimated_cost = estimates[best_service]['cost']

        return RoutingDecision(best_service, estimates, scores)

    def _calculate_score(self, time: float, cost: float,
                        queue_size: int, load_weight: float) -> float:
        """
        Calculate service score

        Formula: score = time × cost^α × (1 + β × queue_size)

        Lower score = better choice

        Args:
            time: Estimated execution time
            cost: Estimated cost
            queue_size: Current queue size
            load_weight: Load balance weight (β)

        Returns:
            Score value
        """
        load_factor = 1.0 + load_weight * queue_size
        score = time * (cost ** self.cost_weight) * load_factor
        return score
