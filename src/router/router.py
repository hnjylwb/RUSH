from typing import List, Dict, Any
from ..models.query import Query, ServiceType
from .query_parser import QueryParser
from .pipeline_splitter import PipelineSplitter
from .pipeline_encoder import PipelineEncoder
from .resource_predictor import ResourcePredictor

class Router:
    """
    Main router that coordinates the entire routing pipeline
    """
    
    def __init__(self):
        # Initialize all components
        self.query_parser = QueryParser()
        self.pipeline_splitter = PipelineSplitter()
        self.pipeline_encoder = PipelineEncoder()
        self.resource_predictor = ResourcePredictor()
        
        # Load service parameters from config
        from ..config.parameters import RUSHParameters
        self.params = RUSHParameters()
        
        # Initialize Athena time predictor
        from .athena_time_predictor import AthenaTimePredictor
        self.athena_time_predictor = AthenaTimePredictor()
    
    def predict_pipeline_resources_with_data(self, query: Query) -> tuple[List[Dict], List[Dict]]:
        """
        Get pipeline-level resource predictions for a query along with original pipeline data
        
        Returns:
            Tuple of (predictions, pipeline_data) where:
            - predictions: List of prediction dictionaries with CPU, memory, IO, duration for each pipeline
            - pipeline_data: List of original pipeline dictionaries with operator information
        """
        try:
            # Step 1: Parse SQL query into execution plan
            execution_plan = self.query_parser.parse_query(query.sql)
            
            # Step 2: Split plan into pipelines based on breakers
            pipelines = self.pipeline_splitter.split_into_pipelines(execution_plan)
            
            # Debug: Print pipeline summary
            print("Pipeline breakdown:")
            self.pipeline_splitter.print_pipeline_summary(pipelines)
            
            # Step 3: Encode each pipeline into feature vectors
            encoded_pipelines = []
            for i, pipeline in enumerate(pipelines):
                features = self.pipeline_encoder.encode_pipeline(pipeline)
                encoded_pipelines.append(features)
                
                # Debug: Print encoding information for each pipeline
                print(f"Pipeline {i+1} encoding:")
                print(f"  Operators in pipeline: {[(op.get('operator', 'unknown'), op.get('phase', 'no_phase')) for op in pipeline.get('operators', [])]}")
                self._print_pipeline_encoding_debug(pipeline, features)
            
            # Step 4: Predict resource usage for each pipeline
            predictions = []
            for i, features in enumerate(encoded_pipelines):
                prediction = self.resource_predictor.predict_resources(features)
                predictions.append(prediction)
                
                # Debug: Print resource predictions for each pipeline
                print(f"Pipeline {i+1} resource predictions:")
                for key, value in prediction.items():
                    print(f"    {key}: {value:.3f}")
                print()
            
            return predictions, pipelines
            
        except Exception as e:
            print(f"Pipeline resource prediction failed: {e}")
            # Return default predictions for fallback
            return [{'cpu_usage': 0.5, 'memory_usage': 0.5, 'io_usage': 0.3, 'duration': 2.0}], []

    def predict_pipeline_resources(self, query: Query) -> List[Dict]:
        """
        Get pipeline-level resource predictions for a query
        
        Returns list of prediction dictionaries with CPU, memory, IO, duration for each pipeline
        """
        predictions, _ = self.predict_pipeline_resources_with_data(query)
        return predictions
    
    def _print_pipeline_encoding_debug(self, pipeline: Dict, features) -> None:
        """Print debug information about pipeline encoding"""
        # Get feature names from encoder
        feature_names = self.pipeline_encoder.get_feature_names()
        
        # Find non-zero features
        non_zero_features = []
        for name, value in zip(feature_names, features):
            if value != 0:
                non_zero_features.append(f"    {name}: {value}")
        
        if non_zero_features:
            print("\n".join(non_zero_features))
        else:
            print("    All features are zero")
        
        # Summary stats
        total_features = len(features)
        non_zero_count = sum(1 for f in features if f != 0)
        print(f"    Total features: {total_features}, Non-zero: {non_zero_count}")
        print()
    
    def route_query(self, query: Query, queues=None) -> ServiceType:
        """
        Route a query to the best service
        
        Args:
            query: Query object with SQL and metadata
            queues: Current service queues for load balancing (optional)
            
        Returns:
            Best service type for this query
        """
        # Use ML pipeline to get resource predictions and pipeline data
        pipeline_predictions, pipeline_data = self.predict_pipeline_resources_with_data(query)
        print(f"ML pipeline predictions obtained for {len(pipeline_predictions)} pipelines")
        
        # Store pipeline information in query for intra-scheduler
        query.set_pipeline_info(pipeline_predictions, pipeline_data)
        
        # Compute resource time slots for all service types
        resource_time_slots = self._compute_resource_time_slots(pipeline_predictions)
        query.set_resource_time_slots(resource_time_slots)
        
        # Score all three services based on predictions and current load
        service_scores = {}
        service_predictions = {}  # Store predictions for inter-scheduler
        
        for service_type in [ServiceType.EC2, ServiceType.LAMBDA, ServiceType.ATHENA]:
            score, estimated_time, estimated_cost = self._calculate_service_score(service_type, pipeline_predictions, query, queues, pipeline_data)
            service_scores[service_type] = score
            
            service_predictions[service_type.value] = {
                'time': estimated_time,
                'cost': estimated_cost
            }
            
            print(f"{service_type.value} score: {score:.3f}")
        
        # Store service predictions in query for inter-scheduler use
        query.set_service_predictions(service_predictions)
        
        # Select service with lowest score (lowest penalty)
        best_service = min(service_scores, key=service_scores.get)
        print(f"Selected best service: {best_service.value}")
        return best_service
    
    def _calculate_service_score(self, service_type: ServiceType, predictions: List[Dict[str, float]], query: Query, queues=None, pipeline_data: List[Dict] = None) -> tuple[float, float, float]:
        """
        Calculate score for a service type using: estimated_time * estimated_cost^a * (1 + b * queue_size)
        Lower score = better choice
        
        Args:
            service_type: The service type to score
            predictions: List of pipeline-level resource predictions
            query: Original query object
            queues: Current service queues (optional)
            
        Returns:
            Tuple of (score, estimated_time, estimated_cost) where score is lower for better choices
        """        
        # Pre-calculate Athena data scanning info to avoid duplication
        athena_scan_info = None
        if service_type == ServiceType.ATHENA and pipeline_data:
            total_scanned_bytes = self._calculate_athena_scanned_data(predictions, pipeline_data)
            scanned_tb = total_scanned_bytes / (1024.0 ** 4)
            estimated_scan_cost = scanned_tb * self.params.ATHENA_DEFAULT_COST_PER_TB
            athena_scan_info = {
                'total_bytes': total_scanned_bytes,
                'scanned_tb': scanned_tb,
                'estimated_cost': estimated_scan_cost
            }
        
        # Step 1: Estimate execution time for this service
        if service_type == ServiceType.LAMBDA:
            estimated_time, lambda_gb_seconds, s3_writes, s3_reads = self._estimate_execution_time(service_type, predictions, pipeline_data, athena_scan_info)
        else:
            estimated_time = self._estimate_execution_time(service_type, predictions, pipeline_data, athena_scan_info)
            lambda_gb_seconds = None
            s3_writes = 0
            s3_reads = 0

        # Step 2: Estimate execution cost for this service (pass S3 access info for Lambda)
        estimated_cost = self._estimate_execution_cost(service_type, predictions, pipeline_data, athena_scan_info, lambda_gb_seconds, s3_writes, s3_reads)

        # Step 3: Get parameters
        cost_performance_param = self.params.COST_PERFORMANCE_TRADEOFF
        
        if service_type == ServiceType.EC2:
            load_balance_param = self.params.EC2_LOAD_BALANCE_FACTOR
        elif service_type == ServiceType.LAMBDA:
            load_balance_param = self.params.LAMBDA_LOAD_BALANCE_FACTOR
        elif service_type == ServiceType.ATHENA:
            load_balance_param = self.params.ATHENA_LOAD_BALANCE_FACTOR
        else:
            load_balance_param = 0.1
        
        # Step 4: Calculate queue size for load balancing
        queue_size = 0
        if queues and service_type in queues:
            queue = queues[service_type]
            queue_size = queue.get_queue_size()
        
        # Step 5: Calculate final score using the formula:
        # score = estimated_time * estimated_cost^a * (1 + b * queue_size)
        load_factor = 1.0 + load_balance_param * queue_size
        score = estimated_time * (estimated_cost ** cost_performance_param) * load_factor
        
        print(f"  Time: {estimated_time:.3f}s, Cost: ${estimated_cost:.6f}, Queue: {queue_size}, Score: {score:.3f}")
        
        return score, estimated_time, estimated_cost
    
    def _estimate_execution_time(self, service_type: ServiceType, predictions: List[Dict[str, float]], pipeline_data: List[Dict] = None, athena_scan_info: Dict = None):
        """
        Estimate total execution time for this service
        
        Args:
            service_type: The service type
            predictions: Pipeline-level resource predictions
            
        Returns:
            For Lambda: tuple of (estimated_time_seconds, lambda_gb_seconds)
            For other services: estimated_time_seconds as float
        """
        if service_type == ServiceType.EC2:
            # VM time: sum all pipeline durations directly
            try:
                total_duration = sum(pred['duration'] for pred in predictions)
            except KeyError as e:
                raise ValueError(f"Missing 'duration' key in pipeline prediction: {e}")
            
            return total_duration
            
        elif service_type == ServiceType.LAMBDA:
            # Lambda time: map pipeline resource demands to Lambda execution model
            # Configuration: 100 instances, 4GB memory each
            lambda_instance_count = 100
            lambda_memory_gb = 4
            
            # Resource mapping:
            # - CPU: 1 Lambda instance (4GB) = 1.2 VM cores = 3.75% of 32-core VM
            # - IO: 1 Lambda instance = 75 MB/s bandwidth = 5.86% of 1.25GB/s VM bandwidth
            lambda_cpu_per_instance = 1.2  # VM cores equivalent per instance
            lambda_io_per_instance = 75  # MB/s per instance
            
            # Total Lambda cluster capacity
            total_lambda_cpu_capacity = lambda_cpu_per_instance * lambda_instance_count  # 120 VM cores equivalent
            total_lambda_io_capacity = lambda_io_per_instance * lambda_instance_count  # 7500 MB/s
            
            # Process each pipeline stage
            pipeline_times = []
            total_gb_seconds = 0.0  # Track GB-seconds for cost calculation
            
            try:
                for pred in predictions:
                    cpu_utilization = pred['cpu_usage']  # CPU utilization (0-1)
                    io_utilization = pred['io_usage']    # IO utilization (0-1)
                    duration = pred['duration']          # Original duration in seconds
                    
                    # Calculate resource demands for this stage
                    # CPU demand = CPU utilization × duration (CPU-seconds)
                    cpu_demand = cpu_utilization * duration  # CPU-seconds needed
                    
                    # IO demand = IO utilization × duration (IO-seconds)
                    io_demand = io_utilization * duration  # IO-seconds needed
                    
                    # Map to Lambda execution time
                    # CPU time estimate = CPU demand / Lambda cluster CPU capacity
                    cpu_time_estimate = cpu_demand / total_lambda_cpu_capacity if total_lambda_cpu_capacity > 0 else 0
                    
                    # IO time estimate = IO demand / Lambda cluster IO capacity
                    io_time_estimate = io_demand / total_lambda_io_capacity if total_lambda_io_capacity > 0 else 0
                    
                    # Take the larger value as the pipeline stage time estimate
                    stage_time_estimate = max(cpu_time_estimate, io_time_estimate)
                    pipeline_times.append(stage_time_estimate)
                    
                    # Calculate GB-seconds for this stage: runtime × memory × instances
                    stage_gb_seconds = stage_time_estimate * lambda_memory_gb * lambda_instance_count
                    total_gb_seconds += stage_gb_seconds
                    
            except KeyError as e:
                raise ValueError(f"Missing required key in pipeline prediction for Lambda mapping: {e}")
            
            # Calculate total pipeline processing time
            total_pipeline_time = sum(pipeline_times)
            
            # Calculate shuffle time, GB-seconds, and S3 access counts (inter-pipeline data transfer)
            shuffle_time = 0.0
            shuffle_gb_seconds = 0.0
            total_s3_reads = 0  # Total S3 read requests
            total_s3_writes = 0  # Total S3 write requests
            
            if pipeline_data and len(pipeline_data) > 1:  # Only multi-pipeline queries need shuffle
                # Shuffle configuration parameters
                shuffle_write_instances = 100  # Number of write instances
                shuffle_read_instances = 100   # Number of read instances
                shuffle_io_speed_mbps = 75     # IO speed per instance in MB/s
                
                # Estimate data volume for each shuffle
                for i in range(len(pipeline_data) - 1):  # Adjacent pipelines need shuffle
                    pipeline_info = pipeline_data[i]
                    
                    # Get actual operator information from pipeline to estimate data volume
                    operators = pipeline_info.get('operators', [])
                    
                    # Calculate estimated cardinality and data size for pipeline output
                    # Use the cardinality of the last operator in pipeline as output cardinality
                    estimated_cardinality = 1000  # Default value
                    total_data_size = 0
                    
                    for op in operators:
                        if 'cardinality' in op:
                            estimated_cardinality = max(estimated_cardinality, op['cardinality'])
                        if 'data_size' in op:
                            total_data_size += op.get('data_size', 0)
                    
                    # Use total data size if available; otherwise estimate from cardinality
                    if total_data_size > 0:
                        shuffle_data_size_bytes = total_data_size
                    else:
                        # Estimate: cardinality × average row size (assume 64 bytes per row)
                        avg_row_size = 64  # bytes per row
                        shuffle_data_size_bytes = estimated_cardinality * avg_row_size
                    
                    shuffle_data_size_mb = shuffle_data_size_bytes / (1024 * 1024)  # Convert to MB
                    
                    # Calculate shuffle time = write time + read time
                    # Write time = data volume / (write instances × IO speed per instance)
                    write_time = shuffle_data_size_mb / (shuffle_write_instances * shuffle_io_speed_mbps)
                    
                    # Read time = data volume / (read instances × IO speed per instance)
                    read_time = shuffle_data_size_mb / (shuffle_read_instances * shuffle_io_speed_mbps)
                    
                    # Single shuffle time
                    single_shuffle_time = write_time + read_time
                    shuffle_time += single_shuffle_time
                    
                    # Single shuffle GB-seconds: shuffle time × memory × instances
                    single_shuffle_gb_seconds = single_shuffle_time * lambda_memory_gb * lambda_instance_count
                    shuffle_gb_seconds += single_shuffle_gb_seconds
                    
                    # Calculate S3 access count: N1 * N2 reads and writes
                    # N1 = instances of previous pipeline, N2 = instances of next pipeline
                    # Currently simplified to 100 instances each
                    n1_instances = lambda_instance_count  # Previous pipeline instances
                    n2_instances = lambda_instance_count  # Next pipeline instances
                    
                    shuffle_s3_writes = n1_instances * n2_instances  # Write request count
                    shuffle_s3_reads = n1_instances * n2_instances   # Read request count
                    
                    total_s3_writes += shuffle_s3_writes
                    total_s3_reads += shuffle_s3_reads
                    
                    # Debug information
                    print(f"    Shuffle {i+1}: {shuffle_data_size_mb:.2f}MB, write: {write_time:.4f}s, read: {read_time:.4f}s, total: {single_shuffle_time:.4f}s")
                    print(f"      GB-s: {single_shuffle_gb_seconds:.6f}, S3 writes: {shuffle_s3_writes}, S3 reads: {shuffle_s3_reads}")
            
            # Return total time, total GB-seconds, and S3 access counts
            total_lambda_time = total_pipeline_time + shuffle_time
            total_lambda_gb_seconds = total_gb_seconds + shuffle_gb_seconds
            
            print(f"    Lambda total: pipeline={total_pipeline_time:.4f}s + shuffle={shuffle_time:.4f}s = {total_lambda_time:.4f}s")
            print(f"    Lambda GB-seconds: pipeline={total_gb_seconds:.6f} + shuffle={shuffle_gb_seconds:.6f} = {total_lambda_gb_seconds:.6f}")
            print(f"    S3 access: {total_s3_writes} writes + {total_s3_reads} reads")
            
            return total_lambda_time, total_lambda_gb_seconds, total_s3_writes, total_s3_reads
            
        elif service_type == ServiceType.ATHENA:
            # Athena time: use specialized time prediction model
            if pipeline_data and athena_scan_info:
                # Use pre-calculated scan info to avoid duplication
                predicted_time = self.athena_time_predictor.predict_execution_time(pipeline_data, athena_scan_info['estimated_cost'])
                return predicted_time
            elif pipeline_data:
                # Fallback: calculate scan info on demand
                total_scanned_bytes = self._calculate_athena_scanned_data(predictions, pipeline_data)
                scanned_tb = total_scanned_bytes / (1024.0 ** 4)
                estimated_scan_cost = scanned_tb * self.params.ATHENA_DEFAULT_COST_PER_TB
                predicted_time = self.athena_time_predictor.predict_execution_time(pipeline_data, estimated_scan_cost)
                return predicted_time
            else:
                # Fallback to pipeline duration sum if no pipeline data
                try:
                    total_duration = sum(pred['duration'] for pred in predictions)
                except KeyError as e:
                    raise ValueError(f"Missing 'duration' key in pipeline prediction: {e}")
                
                return total_duration
    
    def _estimate_execution_cost(self, service_type: ServiceType, predictions: List[Dict[str, float]], pipeline_data: List[Dict] = None, athena_scan_info: Dict = None, lambda_gb_seconds: float = None, s3_writes: int = 0, s3_reads: int = 0) -> float:
        """
        Estimate total execution cost for this service
        
        Args:
            service_type: The service type
            predictions: Pipeline-level resource predictions
            
        Returns:
            Estimated execution cost in dollars
        """
        if service_type == ServiceType.EC2:
            # VM cost: time * hourly_rate * max_resource_ratio
            
            # Get execution time
            try:
                execution_time = sum(pred['duration'] for pred in predictions)
            except KeyError as e:
                raise ValueError(f"Missing 'duration' key in pipeline prediction: {e}")
            
            # Convert hourly cost to per-second cost
            cost_per_second = self.params.EC2_DEFAULT_COST_PER_HOUR / 3600.0
            
            # Calculate maximum resource ratio across all pipelines and resources
            max_resource_ratio = 0.0
            try:
                for pred in predictions:
                    cpu_ratio = pred['cpu_usage']
                    memory_ratio = pred['memory_usage']
                    io_ratio = pred['io_usage']
                    
                    # Take the maximum ratio for this pipeline
                    pipeline_max_ratio = max(cpu_ratio, memory_ratio, io_ratio)
                    
                    # Track the overall maximum across all pipelines
                    max_resource_ratio = max(max_resource_ratio, pipeline_max_ratio)
            except KeyError as e:
                raise ValueError(f"Missing resource key in pipeline prediction: {e}")
            
            # Ensure minimum resource usage for realistic costing
            max_resource_ratio = max(max_resource_ratio, 0.1)
            
            return execution_time * cost_per_second * max_resource_ratio
            
        elif service_type == ServiceType.LAMBDA:
            # Lambda: cost = lambda_gb_seconds × unit_price + S3_access_cost
            # Cost has two parts: 1) Lambda execution cost, 2) S3 access cost
            
            if lambda_gb_seconds is None:
                # This should never happen for Lambda - indicates a bug in time estimation
                raise ValueError("Lambda GB-seconds not provided - this indicates a bug in _estimate_execution_time for Lambda")
            
            # Part 1: Lambda execution cost (GB-seconds × unit price)
            lambda_execution_cost = lambda_gb_seconds * self.params.LAMBDA_COST_PER_SECOND
            
            # Part 2: S3 access cost (reads + writes)
            s3_read_cost = (s3_reads / 1000.0) * self.params.S3_READ_COST_PER_1000_REQUESTS
            s3_write_cost = (s3_writes / 1000.0) * self.params.S3_WRITE_COST_PER_1000_REQUESTS
            s3_total_cost = s3_read_cost + s3_write_cost
            
            # Total cost
            total_cost = lambda_execution_cost + s3_total_cost
            
            print(f"    Lambda cost breakdown:")
            print(f"      Execution: {lambda_gb_seconds:.6f} GB-s × {self.params.LAMBDA_COST_PER_SECOND} = ${lambda_execution_cost:.6f}")
            print(f"      S3 reads: {s3_reads} × ${self.params.S3_READ_COST_PER_1000_REQUESTS}/1000 = ${s3_read_cost:.6f}")
            print(f"      S3 writes: {s3_writes} × ${self.params.S3_WRITE_COST_PER_1000_REQUESTS}/1000 = ${s3_write_cost:.6f}")
            print(f"      Total: ${lambda_execution_cost:.6f} + ${s3_total_cost:.6f} = ${total_cost:.6f}")
            
            return total_cost
            
        elif service_type == ServiceType.ATHENA:
            # Athena: cost based on data scanned (TB * $5/TB)
            
            if athena_scan_info:
                # Use pre-calculated scan info to avoid duplication
                return athena_scan_info['estimated_cost']
            elif pipeline_data:
                # Fallback: calculate on demand
                total_scanned_bytes = self._calculate_athena_scanned_data(predictions, pipeline_data)
                scanned_tb = total_scanned_bytes / (1024.0 ** 4)
                return scanned_tb * self.params.ATHENA_DEFAULT_COST_PER_TB
            else:
                # Final fallback if no data available
                return 0.001
            
        else:
            # Fallback
            return 0.001
    
    def _calculate_athena_scanned_data(self, predictions: List[Dict[str, float]], pipeline_data: List[Dict] = None) -> float:
        """
        Calculate total data scanned by Athena based on SEQ_SCAN operations
        
        Args:
            predictions: Pipeline-level resource predictions (for fallback only)
            pipeline_data: Original pipeline data with operator information
            
        Returns:
            Total scanned data in bytes
        """
        if not pipeline_data:
            # Fallback to old method if no pipeline data available
            try:
                total_io_usage = sum(pred['io_usage'] for pred in predictions)
                estimated_gb = total_io_usage * 1.0
                estimated_bytes = estimated_gb * (1024 ** 3)
                return estimated_bytes
            except KeyError as e:
                raise ValueError(f"Missing 'io_usage' key in pipeline prediction: {e}")
        
        total_scanned_bytes = 0.0
        
        # Process each pipeline to find SEQ_SCAN operations
        for pipeline in pipeline_data:
            operators = pipeline.get('operators', [])
            
            for op in operators:
                op_name = op.get('operator', '').upper()
                
                # Only process SEQ_SCAN operations (actual data scanning)
                if 'SEQ_SCAN' in op_name or 'SCAN' in op_name:
                    # Get cardinality estimate from optimizer
                    cardinality = op.get('estimated_cardinality', 0)
                    if cardinality <= 0:
                        continue
                    
                    # Get table name and accessed columns
                    table_name = self._extract_table_name_from_operator(op)
                    accessed_columns = self._extract_accessed_columns_from_operator(op)
                    
                    if table_name and table_name in self.pipeline_encoder.stats.get('tables', {}):
                        table_stats = self.pipeline_encoder.stats['tables'][table_name]
                        columns_info = table_stats.get('columns', {})
                        
                        # Calculate bytes per row based on accessed columns
                        if accessed_columns:
                            # Use specific columns that are accessed
                            bytes_per_row = sum(
                                columns_info.get(col, {}).get('avg_bytes', 8) 
                                for col in accessed_columns 
                                if col in columns_info
                            )
                        else:
                            # If we can't determine specific columns, assume all columns are scanned
                            bytes_per_row = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values())
                        
                        # Add to total scanned data
                        scan_bytes = cardinality * bytes_per_row
                        total_scanned_bytes += scan_bytes
                        
                        print(f"  SEQ_SCAN on {table_name}: {cardinality} rows × {bytes_per_row} bytes/row = {scan_bytes:.0f} bytes")
                    
                    else:
                        # Fallback: use default row size if no table stats
                        default_bytes_per_row = 32  # Conservative estimate
                        scan_bytes = cardinality * default_bytes_per_row
                        total_scanned_bytes += scan_bytes
                        
                        print(f"  SEQ_SCAN (unknown table): {cardinality} rows × {default_bytes_per_row} bytes/row = {scan_bytes:.0f} bytes")
        
        return total_scanned_bytes
    
    def _extract_table_name_from_operator(self, operator: Dict[str, Any]) -> str:
        """Extract table name from operator extra_info"""
        extra_info = operator.get('extra_info', {})
        
        # Check common fields where table name might be stored
        for field in ['Table', 'Table Name', 'table_name', 'relation', 'table']:
            if field in extra_info:
                return str(extra_info[field]).lower()
        
        # Try to parse from other info
        if 'info' in extra_info:
            info_str = str(extra_info['info']).lower()
            # Look for table names we know about
            for table in ['users', 'orders', 'products', 'employees']:
                if table in info_str:
                    return table
        
        return ''
    
    def _extract_accessed_columns_from_operator(self, operator: Dict[str, Any]) -> List[str]:
        """Extract accessed columns from operator extra_info"""
        extra_info = operator.get('extra_info', {})
        columns = []
        
        # Check for projection information (columns being selected)
        if 'Projections' in extra_info:
            projections = extra_info['Projections']
            if isinstance(projections, list):
                columns.extend(projections)
            elif isinstance(projections, str):
                # Parse projection string, e.g., "id, name, age"
                columns.extend([col.strip() for col in projections.split(',')])
        
        # For SEQ_SCAN, if no specific projections, assume all columns are accessed initially
        # (subsequent operators might filter this down, but for Athena cost estimation,
        # we care about what the scan itself reads from storage)
        
        return list(set(columns)) if columns else []
    
    def _compute_resource_time_slots(self, pipeline_predictions: List[Dict[str, float]]) -> Dict[str, Dict[str, List[float]]]:
        """
        Compute resource time slots for all service types based on pipeline predictions
        
        Args:
            pipeline_predictions: List of pipeline predictions with duration and resource usage
            
        Returns:
            Dict with service-specific resource time slots:
            {
                'EC2': {
                    'cpu_slots': [0.5, 0.3, ...],
                    'memory_slots': [0.4, 0.6, ...], 
                    'io_slots': [0.2, 0.1, ...]
                },
                'LAMBDA': {
                    'instance_slots': [1, 1, 1, ...]
                },
                'ATHENA': {
                    'query_slots': [1, 1, 1, ...]
                }
            }
        """
        time_slot_duration = self.params.TIME_SLOT_DURATION
        
        # Calculate total query duration
        total_duration = sum(pred.get('duration', 2.0) for pred in pipeline_predictions)
        
        # Calculate number of time slots needed
        num_slots = int((total_duration + time_slot_duration - 0.001) / time_slot_duration)
        
        resource_time_slots = {}
        
        # EC2: CPU/Memory/IO time slots
        cpu_slots = [0.0] * num_slots
        memory_slots = [0.0] * num_slots
        io_slots = [0.0] * num_slots
        
        current_time = 0.0
        for pred in pipeline_predictions:
            duration = pred.get('duration', 2.0)
            cpu_usage = pred.get('cpu_usage', 0.5)
            memory_usage = pred.get('memory_usage', 0.5)
            io_usage = pred.get('io_usage', 0.3)
            
            # Map pipeline duration to time slots
            start_slot = int(current_time / time_slot_duration)
            end_time = current_time + duration
            end_slot = int((end_time + time_slot_duration - 0.001) / time_slot_duration)
            
            # Distribute resource usage across time slots
            for slot_idx in range(start_slot, min(end_slot, num_slots)):
                slot_start = slot_idx * time_slot_duration
                slot_end = (slot_idx + 1) * time_slot_duration
                
                # Calculate overlap between pipeline and time slot
                overlap_start = max(current_time, slot_start)
                overlap_end = min(end_time, slot_end)
                overlap_ratio = (overlap_end - overlap_start) / time_slot_duration
                
                # Add weighted resource usage to this slot
                cpu_slots[slot_idx] += cpu_usage * overlap_ratio
                memory_slots[slot_idx] += memory_usage * overlap_ratio
                io_slots[slot_idx] += io_usage * overlap_ratio
            
            current_time += duration
        
        resource_time_slots['EC2'] = {
            'cpu_slots': cpu_slots,
            'memory_slots': memory_slots,
            'io_slots': io_slots
        }
        
        # Lambda: Instance slots (all slots are 1 since each query uses 1 instance)
        resource_time_slots['LAMBDA'] = {
            'instance_slots': [1.0] * num_slots
        }
        
        # Athena: Query slots (all slots are 1 since each query uses 1 query slot)
        resource_time_slots['ATHENA'] = {
            'query_slots': [1.0] * num_slots
        }
        
        return resource_time_slots
    
    def _get_athena_scan_info(self, service_type: ServiceType, predictions: List[Dict[str, float]], pipeline_data: List[Dict] = None) -> Dict:
        """Get Athena scan info to avoid duplicate calculation"""
        if service_type == ServiceType.ATHENA and pipeline_data:
            total_scanned_bytes = self._calculate_athena_scanned_data(predictions, pipeline_data)
            scanned_tb = total_scanned_bytes / (1024.0 ** 4)
            estimated_scan_cost = scanned_tb * self.params.ATHENA_DEFAULT_COST_PER_TB
            return {
                'total_bytes': total_scanned_bytes,
                'scanned_tb': scanned_tb,
                'estimated_cost': estimated_scan_cost
            }
        return {}
