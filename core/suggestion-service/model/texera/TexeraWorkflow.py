import json
from typing import List, Dict, Tuple, Any, Set, Optional
import networkx as nx

from model.Operator import Operator
from model.Port import Port
from model.DataSchema import DataSchema

from model.Workflow import Workflow
from model.llm.interpretation import (
    WorkflowInterpretation,
    LinkInterpretation,
    LinkEndInterpretation,
)
from model.texera.TexeraOperator import TexeraOperator


class Link:
    """Simple class to represent a link between operators."""

    def __init__(self, link_id, src_id, src_port, target_id, target_port):
        self.link_id = link_id
        self.source = type(
            "obj", (object,), {"operator_id": src_id, "port_id": src_port}
        )
        self.target = type(
            "obj", (object,), {"operator_id": target_id, "port_id": target_port}
        )


class TexeraWorkflow(Workflow):
    def __init__(
        self,
        workflow_dict: Dict[str, Any] = None,
        input_schema: Dict[str, List] = None,
        operator_errors: Dict[str, Any] = None,
        wid: int = 0,
        workflow_title: str = "",
    ):
        """
        Initialize a TexeraWorkflow from either a workflow dictionary or directly from operators and links.

        Args:
            workflow_dict: Dictionary representation of the workflow
            input_schema: Dictionary mapping operator IDs to their input schemas
            operator_errors: Dictionary mapping operator IDs to their errors
            wid: Workflow ID
            workflow_title: Title of the workflow
            operators: Dictionary of operator IDs to TexeraOperator objects
            links: List of link objects
        """
        # Initialize with empty values
        self.wid = wid
        self.workflow_title = workflow_title
        self.DAG = nx.DiGraph()
        self.operators = {}
        self.links = []
        self.input_schema = input_schema or {}
        self.operator_errors = operator_errors or {}
        self.workflow_dict = None
        self.workflow_content = None

        # Initialize based on provided parameters
        if workflow_dict is not None:
            # Initialize from workflow dictionary
            self.workflow_dict = workflow_dict
            self.workflow_content = json.dumps(workflow_dict)
            self.initialize_from_dict(workflow_dict)

    def initialize_from_dict(self, workflow_dict: Dict[str, Any]) -> None:
        """Initialize the workflow from a dictionary."""
        self.workflow_content = json.dumps(workflow_dict)
        self.workflow_dict = workflow_dict

        # Extract operators
        content = workflow_dict.get("content", {})
        operators_dict = content.get("operators", [])

        # Reset internal state
        self.operators = {}
        self.links = []

        # Initialize operators
        for op_dict in operators_dict:
            op_id = op_dict.get("operatorID")
            if op_id:
                self.operators[op_id] = TexeraOperator(
                    operator_dict=op_dict,
                    port_indexed_input_schemas=self._get_input_schemas_for_operator(
                        op_id
                    ),
                    error=self.operator_errors.get(op_id, {}),
                )

        # Build the DAG (adds links and completes initialization)
        self._build_dag()

    def _build_dag(self) -> None:
        """Build the DAG from the workflow dictionary's operators and links."""
        self.DAG = nx.DiGraph()

        # Add nodes to DAG
        for operator in self.GetOperators():
            self.DAG.add_node(
                operator.GetId(),
                type=operator.GetType(),
                inputPorts=[port.GetId() for port in operator.GetInputPorts()],
                outputPorts=[port.GetId() for port in operator.GetOutputPorts()],
                error=operator.GetError(),
            )

        # Add links to DAG
        links_dict = self.workflow_dict.get("content", {}).get("links", [])
        for link in links_dict:
            link_id = link.get("linkID")
            source_op_id = link.get("source", {}).get("operatorID")
            src_port_id = link.get("source", {}).get("portID")
            target_op_id = link.get("target", {}).get("operatorID")
            target_port_id = link.get("target", {}).get("portID")

            if source_op_id and target_op_id:
                # Add link to links list
                self.links.append(
                    Link(
                        link_id, source_op_id, src_port_id, target_op_id, target_port_id
                    )
                )

                # Add edge to DAG
                op = self.operators.get(target_op_id)
                schema = None
                if op is not None:
                    schema = op.GetInputSchemaByPortID(target_port_id)

                self.DAG.add_edge(
                    source_op_id,
                    target_op_id,
                    srcPort=src_port_id,
                    targetPort=target_port_id,
                    schema=schema,
                )

    def _build_dag_from_operators_and_links(self) -> None:
        """Build the DAG from the operators and links provided directly."""
        self.DAG = nx.DiGraph()

        # Add nodes to DAG
        for operator in self.GetOperators():
            self.DAG.add_node(
                operator.GetId(),
                type=operator.GetType(),
                inputPorts=[port.GetId() for port in operator.GetInputPorts()],
                outputPorts=[port.GetId() for port in operator.GetOutputPorts()],
                error=operator.GetError(),
            )

        # Add links to DAG
        for link in self.links:
            source_op_id = link.source.operator_id
            src_port_id = link.source.port_id
            target_op_id = link.target.operator_id
            target_port_id = link.target.port_id

            # Add edge to DAG
            op = self.operators.get(target_op_id)
            schema = None
            if op is not None:
                schema = op.GetInputSchemaByPortID(target_port_id)

            self.DAG.add_edge(
                source_op_id,
                target_op_id,
                srcPort=src_port_id,
                targetPort=target_port_id,
                schema=schema,
            )

    def get_all_paths(self) -> List[List[str]]:
        """
        Find all possible paths through the workflow DAG.
        A path is a sequence of operator IDs from a source node (no incoming edges)
        to a sink node (no outgoing edges).

        Returns:
            List of paths, where each path is a list of operator IDs
        """
        # Find source and sink nodes
        source_nodes = [
            node for node in self.DAG.nodes() if self.DAG.in_degree(node) == 0
        ]
        sink_nodes = [
            node for node in self.DAG.nodes() if self.DAG.out_degree(node) == 0
        ]

        if not source_nodes or not sink_nodes:
            return []

        # Use DFS to find all paths
        all_paths = []

        def dfs_paths(current_node, path, visited):
            visited.add(current_node)

            # If we've reached a sink node, add the path
            if current_node in sink_nodes:
                all_paths.append(path.copy())

            # Explore neighbors
            for neighbor in self.DAG.successors(current_node):
                if neighbor not in visited:
                    path.append(neighbor)
                    dfs_paths(neighbor, path, visited.copy())
                    path.pop()

        # Start DFS from each source node
        for source in source_nodes:
            dfs_paths(source, [source], set())

        return all_paths

    def extract_path_workflow(self, path: List[str]) -> "TexeraWorkflow":
        """Extract a subworkflow containing only the operators and links in the given path."""
        # Create a new workflow dict with only the operators in the path
        subworkflow_dict = {"content": {"operators": [], "links": []}}

        # Add operators from the path
        for op_id in path:
            if op_id in self.operators:
                # Find the original operator dict
                for op_dict in self.workflow_dict.get("content", {}).get(
                    "operators", []
                ):
                    if op_dict.get("operatorID") == op_id:
                        subworkflow_dict["content"]["operators"].append(op_dict.copy())
                        break

        # Add links between operators in the path
        for i in range(len(path) - 1):
            source_op_id, target_op_id = path[i], path[i + 1]

            # Find links between these operators
            for link_dict in self.workflow_dict.get("content", {}).get("links", []):
                source = link_dict.get("source", {})
                target = link_dict.get("target", {})

                if (
                    source.get("operatorID") == source_op_id
                    and target.get("operatorID") == target_op_id
                ):
                    subworkflow_dict["content"]["links"].append(link_dict.copy())

        # Create a new TexeraWorkflow with the same input schema and errors but only for this path
        path_input_schema = {
            op_id: self.input_schema.get(op_id, [])
            for op_id in path
            if op_id in self.input_schema
        }

        path_operator_errors = {
            op_id: self.operator_errors.get(op_id)
            for op_id in path
            if op_id in self.operator_errors
        }

        return TexeraWorkflow(
            workflow_dict=subworkflow_dict,
            input_schema=path_input_schema,
            operator_errors=path_operator_errors,
            wid=self.wid,
            workflow_title=f"Path from {path[0]} to {path[-1]}",
        )

    # Interface methods
    def GetWorkflowContent(self) -> str:
        return self.workflow_content

    def GetWorkflowId(self) -> int:
        return self.wid

    def GetOperators(self, types: List[str] = None) -> List["Operator"]:
        if types is None:
            return list(self.operators.values())
        return [op for op in self.operators.values() if op.GetType() in types]

    def get_operators(self) -> List[Dict[str, Any]]:
        """Returns the operator dictionaries from the workflow."""
        return self.workflow_dict.get("content", {}).get("operators", [])

    def get_links(self) -> List[Dict[str, Any]]:
        """Returns the link dictionaries from the workflow."""
        return self.workflow_dict.get("content", {}).get("links", [])

    def TopologicalSort(self) -> List["Operator"]:
        # This is a placeholder for actual topological sort logic
        return list(self.operators.values())

    def GetDAG(self):
        return self.DAG

    def GetSchemaToNextOperatorDistributionMapping(
        self,
    ) -> Dict["DataSchema", Dict[str, int]]:
        result = {}
        for source_op_id, target_op_id, edge_data in self.DAG.edges(data=True):
            schema = edge_data["schema"]
            target_op = self.operators.get(target_op_id)
            target_op_type = target_op.GetType()

            if schema not in result:
                result[schema] = {}
            if target_op_type not in result[schema]:
                result[schema][target_op_type] = 0
            result[schema][target_op_type] += 1
        return result

    def GetOperatorTypeToNextOperatorDistributionMapping(
        self,
    ) -> Dict[str, Dict[str, int]]:
        result = {}
        for source_op_id, target_op_id, edge_data in self.DAG.edges(data=True):
            source_op = self.operators.get(source_op_id)
            source_op_type = source_op.GetType()
            target_op = self.operators.get(target_op_id)
            target_op_type = target_op.GetType()

            if source_op_type not in result:
                result[source_op_type] = {}
            if target_op_type not in result[source_op_type]:
                result[source_op_type][target_op_type] = 0
            result[source_op_type][target_op_type] += 1
        return result

    def GetAdditionPairs(
        self,
    ) -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]:
        results = []
        for source_op_id, target_op_id, edge_data in self.DAG.edges(data=True):
            source_op = self.operators.get(source_op_id)
            target_op = self.operators.get(target_op_id)
            srcPortId = edge_data.get("srcPort")
            targetPortId = edge_data.get("targetPort")

            if source_op is None or target_op is None:
                continue

            srcPort = next(
                (
                    port
                    for port in source_op.GetOutputPorts()
                    if port.GetId() == srcPortId
                ),
                None,
            )
            targetPort = next(
                (
                    port
                    for port in target_op.GetInputPorts()
                    if port.GetId() == targetPortId
                ),
                None,
            )

            if srcPort is None or targetPort is None:
                continue

            results.append(((source_op, srcPort), (target_op, targetPort)))
        return results

    def _get_input_schemas_for_operator(self, operator_id: str) -> List["DataSchema"]:
        """Extract input schemas for a given operator from the input_schema dictionary."""
        if not self.input_schema or operator_id not in self.input_schema:
            return []
        schema_list = self.input_schema.get(operator_id, [])
        return [DataSchema(s) for s in schema_list]

    def __str__(self) -> str:
        operators_str = "\n".join([str(operator) for operator in self.GetOperators()])
        edges_str = "\n".join(
            [f"{source} -> {target}" for source, target in self.DAG.edges()]
        )
        return (
            f"TexeraWorkflow(\n"
            f"  WorkflowID={self.wid},\n"
            f"  Title={self.workflow_title},\n"
            f"  Operators=[\n{operators_str}\n  ],\n"
            f"  DAG Edges=[\n    {edges_str}\n  ]\n"
            f")"
        )

    def VisualizeDAG(self) -> str:
        """Generate a text-based visualization of the workflow DAG."""
        visualization = "Workflow DAG Visualization:\n\nOperators:\n"
        for op_id, operator in self.operators.items():
            visualization += f"  - {op_id} ({operator.GetType()})\n"

        visualization += "\nLinks:\n"
        for link in self.links:
            source_op = link.source.operator_id
            source_port = link.source.port_id
            target_op = link.target.operator_id
            target_port = link.target.port_id
            visualization += (
                f"  - {source_op}:{source_port} → {target_op}:{target_port}\n"
            )

        return visualization

    def ToPydantic(self) -> WorkflowInterpretation:
        return WorkflowInterpretation(
            operators={
                op_id: operator.ToPydantic()
                for op_id, operator in self.operators.items()
            },
            links=[
                LinkInterpretation(
                    source=LinkEndInterpretation(
                        operatorID=link.source.operator_id, portID=link.source.port_id
                    ),
                    target=LinkEndInterpretation(
                        operatorID=link.target.operator_id, portID=link.target.port_id
                    ),
                    linkID=link.link_id,
                )
                for link in self.links
            ],
        )
