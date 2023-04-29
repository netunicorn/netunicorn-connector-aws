from __future__ import annotations

import json
import logging
from logging import Logger
from typing import Optional, Tuple, Any

import yaml
from netunicorn.base.architecture import Architecture
from netunicorn.base.deployment import Deployment
from netunicorn.base.environment_definitions import DockerImage, ShellExecution
from netunicorn.base.nodes import CountableNodePool, Node, Nodes, UncountableNodePool
from returns.result import Failure, Result, Success

from netunicorn.director.base.connectors.protocol import (
    NetunicornConnectorProtocol,
)
from netunicorn.director.base.connectors.types import StopExecutorRequest


class ConnectorTemplate(NetunicornConnectorProtocol):
    def __init__(
            self,
            connector_name: str,
            configuration: str | None,
            netunicorn_gateway: str,
            logger: Optional[Logger] = None
    ):
        # system-unique name for this instance of the connector
        # just store it
        self.connector_name = connector_name

        # connector-specific configuration
        self.configuration = configuration
        # for example, you can wait for a filename or JSON to be parsed
        with open(self.configuration) as f:
            self.configuration = yaml.safe_load(f)
        # or
        self.configuration = json.loads(self.configuration)

        # default netunicorn gateway address
        # should be provided as environment variable NETUNICORN_GATEWAY_ENDPOINT to the executor
        self.netunicorn_gateway = netunicorn_gateway

        # optional logging.Logger instance
        self.logger = logger or logging.getLogger(__name__)

    async def initialize(self, *args: Any, **kwargs: Any) -> None:
        # initialize the connector
        # for example, create async http client to rest api or similar instantiation
        pass

    async def health(self, *args: Any, **kwargs: Any) -> Tuple[bool, str]:
        # implement internal checks if connector is healthy
        # e.g., for remote API connector check if remote API is reachable
        pass

    async def shutdown(self, *args: Any, **kwargs: Any) -> None:
        # shutdown the connector, e.g., close http client
        pass

    async def get_nodes(
            self,
            username: str,
            authentication_context: Optional[dict[str, str]] = None,
            *args: Any,
            **kwargs: Any,
    ) -> Nodes:
        # you should implement mapping between infrastructure nodes and netunicorn Node abstraction
        # and return Nodes object
        # if node amount is possible infinite, you can return UncountableNodePool object with node generator
        # otherwise, return CountableNodePool object with list of nodes
        # you can require users to provide any additional information in authentication_context
        # e.g., private tokens or API keys

        # example for a real infrastructure with 3 nodes:
        return CountableNodePool(
            [
                Node(
                    name=f"node-{i}",
                    properties={'cpu': 4, 'memory': 16, 'gpu': 0},
                    architecture=Architecture.LINUX_AMD64,
                )
                for i in range(3)
            ]
        )

        # example for a cloud infrastructure with nodes assigned during execution
        # where nodes could be either CPU or GPU
        return UncountableNodePool(
            node_template=[
                Node(
                    name=f"node-cpu-",
                    properties={'cpu': 4, 'memory': 16, 'gpu': 0},
                    architecture=Architecture.LINUX_AMD64,
                ),
                Node(
                    name=f"node-gpu-",
                    properties={'cpu': 4, 'memory': 16, 'gpu': 1},
                    architecture=Architecture.LINUX_AMD64,
                ),
            ]
        )

    async def deploy(
            self,
            username: str,
            experiment_id: str,
            deployments: list[Deployment],
            deployment_context: Optional[dict[str, str]],
            authentication_context: Optional[dict[str, str]] = None,
            *args: Any,
            **kwargs: Any,
    ) -> dict[str, Result[Optional[str], str]]:
        # you should implement deployment of the given list of deployments on the infrastructure
        # deployment means preparation of the environment (such as docker image pull or executor and dependencies installation)
        # each deployment is a mapping of a pipeline to a node
        # you can require users to provide any additional information in authentication_context (e.g., API key)
        # and deployment_context (e.g., deployment flags)
        # Deployments are of one of the following types:
        # - DockerImage -- docker image name is provided, you should pull the image to a given node
        # - ShellExecution -- you should execute all commands given in a deployment.environment_definition.commands on a given node
        #   and ensure that netunicorn-executor is installed
        # for each deployment, you should return a Result[optional success message, error message]
        # combined them together in a dictionary of deployment.executor_id -> Result
        for deployment in deployments:
            print(deployment.executor_id)
            if isinstance(deployment.environment_definition, DockerImage):
                # pull docker image
                print(deployment.environment_definition.image)
            elif isinstance(deployment.environment_definition, ShellExecution):
                print(deployment.environment_definition.commands)
        return {
            deployment.executor_id: Success("success message")
            for deployment in deployments
        }
        pass

    async def execute(
            self,
            username: str,
            experiment_id: str,
            deployments: list[Deployment],
            execution_context: Optional[dict[str, str]],
            authentication_context: Optional[dict[str, str]] = None,
            *args: Any,
            **kwargs: Any,
    ) -> dict[str, Result[Optional[str], str]]:
        # for a given list of deployments, you should start executors on the infrastructure
        # You should use deployment.environment_definition.runtime_context for additional port mappings
        # (in case of Docker containers) or user-defined environment variables
        # For all deployments, you should also provide the next environment variables:
        #   - NETUNICORN_GATEWAY_ENDPOINT: netunicorn gateway endpoint (default is given during connector initialization)
        #   - NETUNICORN_EXECUTOR_ID: given in the deployment.executor_id
        #   - NETUNICORN_EXPERIMENT_ID: given in the parameters of this method

        # deployment.prepared defines if the deployment was successful
        # you don't need to deploy failed deployments but can examine them as well

        # example with docker containers
        result = {}
        for deployment in deployments:
            if not deployment.prepared:
                result[deployment.executor_id] = Failure("skipped")
            if not isinstance(deployment.environment_definition, DockerImage):
                result[deployment.executor_id] = Failure("not a docker image")

            assert deployment.node.name == 'local'
            print(
                f"docker run -d -p {deployment.environment_definition.runtime_context.ports_mapping} "
                f"-e NETUNICORN_GATEWAY_ENDPOINT={self.netunicorn_gateway} "
                f"-e NETUNICORN_EXECUTOR_ID={deployment.executor_id} "
                f"-e NETUNICORN_EXPERIMENT_ID={experiment_id} "
                " -e ".join(
                    f'{x}={y}' for x, y
                    in deployment.environment_definition.runtime_context.environment_variables.items()
                ),
                f" --name {deployment.executor_id}"
                f" {deployment.environment_definition.image}"
            )
            result[deployment.executor_id] = Success(None)
        return result

    async def stop_executors(
            self,
            username: str,
            requests_list: list[StopExecutorRequest],
            cancellation_context: Optional[dict[str, str]],
            authentication_context: Optional[dict[str, str]] = None,
            *args: Any,
            **kwargs: Any,
    ) -> dict[str, Result[Optional[str], str]]:
        # cancel executors on the infrastructure
        # user can provide additional context via cancellation_context and authentication_context
        # e.g.:
        print(cancellation_context.get('reason'))

        # example for docker infrastructure
        result = {}
        for request in requests_list:
            assert request['node_name'] == 'local'
            print(f"docker stop {request['executor_id']}")
            result[request['executor_id']] = Success(None)

        return result
