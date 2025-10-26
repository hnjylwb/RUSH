from typing import List, Dict, Any

class PipelineSplitter:
    """
    Splits DuckDB execution plan into pipelines based on pipeline breaker operators
    Uses DFS traversal to handle the tree structure properly
    """
    
    def __init__(self):
        # Define pipeline breaker operators that force pipeline boundaries
        self.pipeline_breakers = {
            'HASH_GROUP_BY',    # Aggregation requires all input data
            'ORDER_BY',         # Sorting requires all input data
        }
        # HASH_JOIN is special - it's a breaker for build phase but not probe phase
    
    def split_into_pipelines(self, execution_plan) -> List[Dict[str, Any]]:
        """
        Split execution plan into pipelines with correct breaker handling
        
        Pipeline definition: Each breaker is both the end of one pipeline and 
        the start of the next pipeline.
        """
        if not execution_plan:
            raise Exception("Empty execution plan provided to pipeline splitter")
        
        # First, flatten the tree into a linear sequence of operators
        operators_sequence = []
        if isinstance(execution_plan, list):
            for plan_node in execution_plan:
                self._flatten_tree_to_sequence(plan_node, operators_sequence)
        else:
            self._flatten_tree_to_sequence(execution_plan, operators_sequence)
        
        if not operators_sequence:
            raise Exception(f"No operators found in execution plan: {execution_plan}")
        
        # Now split the sequence into pipelines at breaker boundaries
        pipelines = self._split_sequence_into_pipelines(operators_sequence)
        
        if not pipelines:
            raise Exception(f"Failed to create any pipelines from execution plan")
        
        return pipelines
    
    def _flatten_tree_to_sequence(self, node: Dict[str, Any], sequence: List[Dict]):
        """
        Flatten the execution tree into a linear sequence (execution order)
        Uses post-order traversal: children first, then current node
        Special handling for pipeline breakers: each breaker has sink and source phases
        HASH_JOIN uses build/probe instead of sink/source
        """
        if not isinstance(node, dict) or 'name' not in node:
            return
        
        operator_name = node['name'].strip()
        children = node.get('children', [])
        
        # Special handling for HASH_JOIN: right child (build) first, then left child (probe)
        if operator_name == 'HASH_JOIN' and len(children) >= 2:
            # Process right child first (build phase)
            self._flatten_tree_to_sequence(children[1], sequence)
            
            # Add the HASH_JOIN build phase (pipeline end)
            build_operator_info = {
                'operator': operator_name,
                'estimated_cardinality': self._extract_cardinality(node),
                'extra_info': node.get('extra_info', {}),
                'phase': 'build'  # Build phase ends pipeline
            }
            sequence.append(build_operator_info)
            
            # Process left child (probe phase)
            self._flatten_tree_to_sequence(children[0], sequence)
            
            # Add HASH_JOIN probe phase (pipeline start)
            probe_operator_info = {
                'operator': operator_name,
                'estimated_cardinality': self._extract_cardinality(node),
                'extra_info': node.get('extra_info', {}),
                'phase': 'probe'  # Probe phase starts next pipeline
            }
            sequence.append(probe_operator_info)
            
        # Special handling for other pipeline breakers
        elif operator_name in self.pipeline_breakers:
            # Process children first (post-order)
            for child in children:
                self._flatten_tree_to_sequence(child, sequence)
            
            # Add sink phase (end of current pipeline)
            sink_operator_info = {
                'operator': operator_name,
                'estimated_cardinality': self._extract_cardinality(node),
                'extra_info': node.get('extra_info', {}),
                'phase': 'sink'
            }
            sequence.append(sink_operator_info)
            
            # Add source phase (start of next pipeline)
            source_operator_info = {
                'operator': operator_name,
                'estimated_cardinality': self._extract_cardinality(node),
                'extra_info': node.get('extra_info', {}),
                'phase': 'source'
            }
            sequence.append(source_operator_info)
            
        else:
            # For other operators: children first (post-order), then current operator
            for child in children:
                self._flatten_tree_to_sequence(child, sequence)
            
            # Add current operator after children
            operator_info = {
                'operator': operator_name,
                'estimated_cardinality': self._extract_cardinality(node),
                'extra_info': node.get('extra_info', {})
            }
            sequence.append(operator_info)
    
    def _split_sequence_into_pipelines(self, operators: List[Dict]) -> List[Dict[str, Any]]:
        """
        Split operator sequence into pipelines with correct breaker handling
        
        Pipeline boundaries:
        - Sink phase or build phase: ends current pipeline
        - Source phase or probe phase: starts next pipeline
        - Pipeline starts: SEQ_SCAN, source phase, or probe phase
        """
        pipelines = []
        self.pipeline_counter = 0
        
        if not operators:
            return pipelines
        
        # Find pipeline end positions (sink or build phases)
        end_positions = []
        
        for i, op in enumerate(operators):
            operator_name = op['operator']
            phase = op.get('phase', '')
            
            if ((operator_name in self.pipeline_breakers and phase == 'sink') or
                (operator_name == 'HASH_JOIN' and phase == 'build')):
                end_positions.append(i)
        
        # Create pipelines between end positions
        start_idx = 0
        
        for end_pos in end_positions:
            # Create pipeline from start_idx to end_pos (inclusive)
            pipeline_ops = operators[start_idx:end_pos + 1]
            self._create_pipeline_from_ops(pipelines, pipeline_ops)
            
            # Find next valid start position (should be source/probe phase or SEQ_SCAN)
            next_start = end_pos + 1
            while next_start < len(operators):
                next_op = operators[next_start]
                next_phase = next_op.get('phase', '')
                next_name = next_op['operator']
                
                # Valid start: source/probe phase or SEQ_SCAN
                if (next_phase in ['source', 'probe'] or 
                    next_name in ['SEQ_SCAN', 'READ_PARQUET'] or 
                    'SCAN' in next_name):
                    break
                next_start += 1
            
            start_idx = next_start
        
        # Create final pipeline from last end to finish (or from start if no ends)
        if start_idx < len(operators):
            final_ops = operators[start_idx:]
            self._create_pipeline_from_ops(pipelines, final_ops)
        
        return pipelines
    
    def _extract_cardinality(self, node: Dict[str, Any]) -> int:
        """Extract estimated cardinality from node extra_info"""
        try:
            if 'extra_info' in node and 'Estimated Cardinality' in node['extra_info']:
                cardinality_str = node['extra_info']['Estimated Cardinality']
                return int(cardinality_str)
        except (ValueError, TypeError, KeyError):
            pass
        return 0
    
    def _create_pipeline_from_ops(self, pipelines: List[Dict], operators: List[Dict]):
        """Create a pipeline from a list of operators"""
        if not operators:
            return
        
        self.pipeline_counter += 1
        
        # Calculate pipeline metadata
        total_cardinality = sum(op.get('estimated_cardinality', 0) for op in operators)
        has_breaker = any(op['operator'] in self.pipeline_breakers for op in operators)
        
        pipeline = {
            'pipeline_id': self.pipeline_counter,
            'operators': operators,
            'operator_count': len(operators),
            'total_estimated_cardinality': total_cardinality,
            'has_breaker': has_breaker,
            'breaker_operator': None,
            'complexity_score': self._calculate_complexity_score(operators)
        }
        
        pipelines.append(pipeline)
    
    def _calculate_complexity_score(self, operators: List[Dict]) -> float:
        """
        Calculate complexity score for a pipeline based on operators and cardinality
        Higher score = more complex pipeline
        """
        score = 0.0
        
        for op in operators:
            op_name = op['operator']
            cardinality = op.get('estimated_cardinality', 0)
            
            # Base complexity by operator type
            if op_name in self.pipeline_breakers:
                score += 10.0  # High complexity for breakers
            elif op_name == 'HASH_JOIN':
                score += 8.0   # High complexity for hash joins
            elif op_name in ['PROJECTION', 'FILTER']:
                score += 1.0   # Low complexity for simple ops
            elif 'READ_PARQUET' in op_name or 'SEQ_SCAN' in op_name:
                score += 2.0   # Medium complexity for I/O ops
            else:
                score += 5.0   # Unknown operators get medium-high score
            
            # Add cardinality factor (log scale to avoid domination)
            if cardinality > 0:
                import math
                score += math.log10(cardinality + 1) * 0.5
        
        return score
    
    def print_pipeline_summary(self, pipelines: List[Dict[str, Any]]) -> str:
        """
        Print concise pipeline summary showing operator names with HASH_JOIN phases
        Returns formatted string for easy reading
        """
        summary_lines = []
        summary_lines.append(f"Pipelines ({len(pipelines)}):")
        
        for i, pipeline in enumerate(pipelines, 1):
            # Get operator names with phase info for all pipeline breakers
            op_names = []
            for op in pipeline['operators']:
                op_name = op['operator']
                # Show phase info for all operators that have phases
                if 'phase' in op and op['phase']:
                    op_names.append(f"{op_name}({op['phase']})")
                else:
                    op_names.append(op_name)
            
            ops_str = ' → '.join(op_names)
            
            # Mark breaker pipelines
            breaker_mark = " [BREAKER]" if pipeline['has_breaker'] else ""
            
            summary_lines.append(f"  P{i}: {ops_str}{breaker_mark}")
        
        result = '\n'.join(summary_lines)
        print(result)
        return result
