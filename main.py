from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Literal, TypedDict, Union

import yaml
from google.cloud import workflows


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

    def __post_init__(self):
        if "{{" in self.args["text"]:
            self.args["text"] = self.args["text"].replace("{{", "${").replace("}}", "}")


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
class Node:
    id: str
    task: Union[SleepTask]


@dataclass
class Edge:
    source: str
    target: str
    condition: Literal["SUCCESS", "FAILURE", "SKIPPED"]


@dataclass
class WorkflowData:
    nodes: list[Node]
    edges: list[Edge]


def compile_to_workflows(data: WorkflowData) -> dict:
    # Workflow definition
    workflow = {
        "params": ["args"],
        "steps": [{
            "init": {
                "assign": [{
                    "node_results": {}
                }]
            }
        }]
    }

    # Get initial nodes, i.e. nodes without incoming edges
    initial_nodes = [
        node for node in data.nodes
        if not any(edge.target == node.id for edge in data.edges)
    ]

    def new_step(node: Node) -> dict:
        return {
            f"execute_{node.id}": {
                "try": {
                    "steps": [
                        {
                            f"call_node_{node.id}": {
                                "call": str(node.task),
                                "result": f"{node.id}_results",
                                "args": node.task.args
                            }
                        },
                        {
                            f"assign_success_node_{node.id}": {
                                "assign": [
                                    {f"node_results[\"{node.id}\"]": {}},
                                    {f"node_results[\"{node.id}\"].status": "SUCCESS"},
                                    {f"node_results[\"{node.id}\"].output": f"${{{node.id}_results}}"}
                                ]
                            }

                        }
                    ]
                },
                "except": {
                    "as": "e",
                    "steps": [
                        {
                            f"assign_error_node_{node.id}": {
                                "assign": [
                                    {f"node_results[\"{node.id}\"]": {}},
                                    {f"node_results[\"{node.id}\"].status": "FAILURE"},
                                    {f"node_results[\"{node.id}\"].output": "${e}"}
                                ]
                            }
                        }
                    ]
                }
            }
        }

    # Add initial nodes to the workflow as parallel branches, if multiple
    if len(initial_nodes) > 1:
        workflow["steps"].append({"run_initial_nodes": {
            "parallel": {
                "shared": ["node_results"],
                "branches": [
                    {
                        f"branch_{node.id}": {
                            "steps": [new_step(node)]
                        }
                    }
                    for node in initial_nodes
                ]
            }
        }})
    else:
        workflow["steps"].append(new_step(initial_nodes[0]))

    def create_switch_condition(node: Node, edges: list[Edge], embedded: bool = True):
        conditions = {
            "condition": "${" + " and ".join([
                f"node_results[\"{edge.source}\"].status == \"{edge.condition}\""
                for edge in edges
            ]) + "}",
        }

        if embedded:
            conditions["steps"] = [new_step(node)]
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
            # workflow["steps"].append({
            #     f"branch_{node.id}": {
            #         "steps": [new_step(node)]
            #     }
            # })

    # Add nodes with incoming edges
    # for node in data.nodes:
    #     if any(edge.target == node.id for edge in data.edges):
    #         workflow["steps"].append(new_step(node))

    # Add pipeline end step
    workflow["steps"].append({
        "workflow_end": {
            "return": {
                "node_results": "${node_results}"
            }
        }
    })

    return {
        "main": workflow
    }


def main():
    # data = WorkflowData(
    #     nodes=[
    #         Node(id="n1", type="sleep-task", config=SleepTaskConfig(seconds=1)),
    #         Node(id="n2", type="sleep-task", config=SleepTaskConfig(seconds=1)),
    #         Node(id="n3", type="sleep-task", config=SleepTaskConfig(seconds=1)),

    #         Node(id="n4", type="sleep-task", config=SleepTaskConfig(seconds=1)),
    #         Node(id="n5", type="sleep-task", config=SleepTaskConfig(seconds=1)),
    #         Node(id="n6", type="sleep-task", config=SleepTaskConfig(seconds="aaa")),
    #         Node(id="n7", type="sleep-task", config=SleepTaskConfig(seconds=1)),
    #         Node(id="n8", type="sleep-task", config=SleepTaskConfig(seconds=1))
    #     ],
    #     edges=[
    #         Edge(source="n1", target="n3", condition="SUCCESS"),
    #         Edge(source="n2", target="n3", condition="SUCCESS"),

    #         Edge(source="n4", target="n5", condition="SUCCESS"),
    #         Edge(source="n5", target="n6", condition="SUCCESS"),
    #         Edge(source="n5", target="n7", condition="SUCCESS"),
    #         Edge(source="n6", target="n8", condition="FAILURE"),
    #         Edge(source="n7", target="n8", condition="SUCCESS")
    #     ],
    # )
    data = WorkflowData(
        nodes=[
            # Node(id="n1", task=SleepTask(args={"seconds": 1})),
            Node(id="n1", task=HttpTask(args={
                "method": "GET",
                "url": "https://google.com",
            })),

            Node(id="n2", task=LogTask(args={"text": "{{ n1.output.code }}"})),
        ],
        edges=[
            Edge(source="n1", target="n2", condition="SUCCESS")
        ],
    )

    with open("compilated.yaml", "w") as f:
        yaml.dump(compile_to_workflows(data), f)


if __name__ == "__main__":
    main()
