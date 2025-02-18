from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Literal, TypedDict

import yaml
from google.cloud import workflows


def callable_node_dict(node: "Node"):
    return {
        f"call_node_{node.id}": {
            "call": str(node.task),
            "result": f"{node.id}_results",
            "args": node.task.args
        }
    }


@dataclass
class BaseTask(ABC):
    args: dict
    name: str

    def __str__(self):
        return self.name


class SleepTaskArgs(TypedDict):
    seconds: int


@dataclass
class SleepTask(BaseTask):
    args: SleepTaskArgs
    name: str = "sys.sleep"


class LogTaskArgs(TypedDict):
    text: str
    data: Any


@dataclass
class LogTask(BaseTask):
    args: LogTaskArgs
    name: str = "sys.log"


class HttpTaskArgs(TypedDict):
    url: str
    headers: dict
    body: dict
    query: dict
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]


@dataclass
class HttpTask(BaseTask):
    args: HttpTaskArgs
    name: str = "http.request"


@dataclass
class ForLoopTask:
    nodes: list["Node"]
    key: str
    iterable: list[Any]
    parallel: int = 1


@dataclass
class SetVariablesTask:
    data: dict


dtypes_list = str | int | float | bool | list | dict


@dataclass
class Param:
    value: None
    dtype: dtypes_list
    name: str

    def __post_init__(self):
        if not isinstance(self.value, self.dtype | None):
            raise Exception(f"'{self.value}' is not of type {self.dtype}")


@dataclass
class Variable:
    value: None
    dtype: dtypes_list
    name: str

    def __post_init__(self):
        if not isinstance(self.value, self.dtype | None):
            raise Exception(f"'{self.value}' is not of type {self.dtype}")

    # def set_value(self):
    #     return {
    #         f"assign"
    #     }


@dataclass
class Node:
    id: str
    task: SleepTask | LogTask | HttpTask | ForLoopTask | SetVariablesTask  # | AppendVariableTask

    def get_as_dict(self):
        if isinstance(self.task, BaseTask):
            return {
                f"execute_{self.id}": {
                    "try": {
                        "steps": [
                            {
                                f"call_node_{self.id}": {
                                    "call": str(self.task),
                                    "result": f"{self.id}_results",
                                    "args": self.task.args
                                }
                            },
                            {
                                f"assign_success_node_{self.id}": {
                                    "assign": [
                                        {f"results[\"{self.id}\"]": {}},
                                        {f"results[\"{self.id}\"].status": "SUCCESS"},
                                        {f"results[\"{self.id}\"].output": f"${{{self.id}_results}}"}
                                    ]
                                }

                            }
                        ]
                    },
                    "except": {
                        "as": "e",
                        "steps": [
                            {
                                f"assign_error_node_{self.id}": {
                                    "assign": [
                                        {f"results[\"{self.id}\"]": {}},
                                        {f"results[\"{self.id}\"].status": "FAILURE"},
                                        {f"results[\"{self.id}\"].output": "${e}"}
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        elif isinstance(self.task, ForLoopTask):
            fix_var_name = f"${{{self.task.iterable}}}" if type(self.task.iterable) is str else self.task.iterable

            return {
                f"loop_{self.id}": {
                    "parallel": {
                        "shared": ["results"],
                        "for": {
                            "value": self.task.key,
                            "in": fix_var_name,
                            "steps": [node.get_as_dict() for node in self.task.nodes]
                        }
                    }
                }
            }
        elif isinstance(self.task, SetVariablesTask):
            return {
                f"set_variables_{self.id}": {
                    "assign": [
                        {f'variables["${k}"]': v} for k, v in self.task.data.items()
                    ]
                }
            }


@dataclass
class Edge:
    source: str
    target: str
    condition: Literal["SUCCESS", "FAILURE", "SKIPPED"]


@dataclass
class WorkflowData:
    nodes: list[Node]
    edges: list[Edge]
    params: list[Param]
    variables: list[Variable]


def compile_to_workflows(data: WorkflowData) -> dict:
    # Workflow definition
    workflow = {
        "params": ["args"],
        "steps": [{
            "init": {
                "assign": [
                    {
                        "results": {},
                    },
                    {
                        "variables": {var.name: var.value for var in data.variables},
                    },
                    {
                        "params": {var.name: var.value for var in data.params},
                    }
                ]
            }
        }]
    }

    # Get initial nodes, i.e. nodes without incoming edges
    initial_nodes = [
        node for node in data.nodes
        if not any(edge.target == node.id for edge in data.edges)
    ]

    # Add initial nodes to the workflow as parallel branches, if multiple
    if len(initial_nodes) > 1:
        workflow["steps"].append({"run_initial_nodes": {
            "parallel": {
                "shared": ["results"],
                "branches": [
                    {
                        f"branch_{node.id}": {
                            "steps": [node.get_as_dict()]
                        }
                    }
                    for node in initial_nodes
                ]
            }
        }})
    else:
        workflow["steps"].append(initial_nodes[0].get_as_dict())

    def create_switch_condition(node: Node, edges: list[Edge], embedded: bool = True):
        conditions = {
            "condition": "${" + " and ".join([
                f"results[\"{edge.source}\"].status == \"{edge.condition}\""
                for edge in edges
            ]) + "}",
        }

        if embedded:
            conditions["steps"] = [node.get_as_dict()]
        else:
            conditions["next"] = f"branch_{node.id}"

        return {
            f"switch_{node.id}": {
                "switch": [conditions]
            }
        }

    # Add nodes with incoming edges
    for node in data.nodes:
        edges = [edge for edge in data.edges if edge.target == node.id]

        # If there is any edge
        if any(edges):
            workflow["steps"].append(create_switch_condition(node, edges, embedded=True))

    # Add pipeline end step
    workflow["steps"].append({
        "workflow_end": {
            "return": {
                "results": "${results}"
            }
        }
    })

    return {
        "main": workflow
    }


def main():
    data = WorkflowData(
        nodes=[
            Node(id="n1", task=HttpTask(args={
                "method": "GET",
                "url": "https://google.com",
            })),

            Node(id="n2", task=LogTask(args={"text": "ok"})),
            Node(
                id="l1", task=ForLoopTask(
                    key="my_key",
                    iterable=[1, 2, 3],
                    nodes=[
                        Node(id="l1_n1", task=SleepTask(args={"seconds": 1})),
                        Node(id="set_my_var", task=SetVariablesTask({"my_var": "ok", "my_var_2": "ok2"}))
                    ]
                )
            )
        ],
        edges=[
            Edge(source="n1", target="n2", condition="SUCCESS")
        ],
        variables=[
            Variable(
                value=None,
                dtype=str,
                name="my_var"
            )
        ],
        params=[
            Param(
                value=None,
                dtype=str,
                name="my_param"
            )
        ]
    )

    with open("compilated.yaml", "w") as f:
        yaml.dump(compile_to_workflows(data), f)


if __name__ == "__main__":
    main()
