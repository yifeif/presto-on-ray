from typing import Optional

from mypy_extensions import TypedDict


class ConfConfig(TypedDict):
    is_coordinator: bool
    http_server_http_port: int
    discovery_uri: str
    query_max_memory: Optional[int] = 5  # GB
    query_max_memory_per_node: Optional[int] = 1  # GB
    query_max_total_memory_per_node: Optional[int] = 2  # GB


class NodeConfig(TypedDict):
    """Based off of Instances model in models.py"""

    environment: str
    node_id: str
    data_dir: str


class Config(TypedDict):
    node_config: NodeConfig
    conf_config: ConfConfig
