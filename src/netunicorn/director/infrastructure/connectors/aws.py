from __future__ import annotations

import asyncio
import logging
import os
from copy import deepcopy
from logging import Logger
from typing import Optional, Tuple, Any, NoReturn

import boto3
import yaml
from netunicorn.base.architecture import Architecture
from netunicorn.base.deployment import Deployment
from netunicorn.base.environment_definitions import DockerImage
from netunicorn.base.nodes import Node, Nodes, UncountableNodePool
from returns.result import Failure, Result, Success
from returns.pipeline import is_successful

from netunicorn.director.base.connectors.protocol import (
    NetunicornConnectorProtocol,
)
from netunicorn.director.base.connectors.types import StopExecutorRequest


class AWSFargate(NetunicornConnectorProtocol):
    def __init__(
        self,
        connector_name: str,
        configuration: str | None,
        netunicorn_gateway: str,
        logger: Optional[Logger] = None,
    ):
        self.connector_name = connector_name

        # default netunicorn gateway address
        # should be provided as environment variable NETUNICORN_GATEWAY_ENDPOINT to the executor
        self.netunicorn_gateway = netunicorn_gateway

        # optional logging.Logger instance
        self.logger = logger or logging.getLogger(__name__)

        with open(configuration) as f:
            self.configuration = yaml.safe_load(f)

        self.access_key = (
            os.environ.get("AWS_ACCESS_KEY_ID", None)
            or self.configuration["netunicorn.aws.access_key"]
        )
        self.secret_key = (
            os.environ.get("AWS_SECRET_ACCESS_KEY", None)
            or self.configuration["netunicorn.aws.secret_key"]
        )
        self.default_region = (
            os.environ.get("AWS_DEFAULT_REGION", None)
            or self.configuration.get("netunicorn.aws.default_region", None)
            or "us-east-1"
        )
        self.cluster = (
            os.environ.get("AWS_CLUSTER", None)
            or self.configuration.get("netunicorn.aws.cluster", None)
            or "default"
        )

        self.subnet = os.environ.get("AWS_SUBNET", None) or self.configuration.get(
            "netunicorn.aws.subnet", None
        )
        if not self.subnet:
            self.logger.info("No subnet provided, creating new...")
            self.subnet = self._create_subnet()

        self._create_node_template(
            self.configuration.get("netunicorn.aws.containers.configurations", None)
        )
        self.allow_custom_containers = bool(
            self.configuration.get("netunicorn.aws.containers.allow_custom", False)
        )

        self.soft_limit = self.configuration.get("netunicorn.aws.containers.soft_limit", None)

        self.ecs_client = boto3.client(
            "ecs",
            region_name=self.default_region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        clusters = self.__get_cluster_names()
        if self.cluster not in clusters:
            self.logger.info(f"Cluster {self.cluster} does not exist, creating...")
            self.ecs_client.create_cluster(
                clusterName=self.cluster,
                capacityProviders=["FARGATE"],
                defaultCapacityProviderStrategy=[
                    {
                        "capacityProvider": "FARGATE",
                    }
                ],
            )

        self.cleaner_task: Optional[asyncio.Task] = None
        self.logger.info("AWS Fargate connector initialized")

    def __get_cluster_names(self) -> set[str]:
        clusters = self.ecs_client.list_clusters()
        result = set()
        for cluster in clusters["clusterArns"]:
            result.add(cluster.split("/")[1])
        return result

    async def __periodic_cleaner(self) -> NoReturn:
        """
        Periodically clean stopped containers and deregistered tasks
        """
        self.logger.info("Starting AWS Fargate cleaner")
        while True:
            try:
                definitions = self.ecs_client.list_task_definitions(
                    status="INACTIVE",
                )

                if definitions["taskDefinitionArns"]:
                    self.ecs_client.delete_task_definitions(
                        taskDefinitions=definitions["taskDefinitionArns"]
                    )
                    self.logger.debug(f"Cleaned {len(definitions['taskDefinitionArns'])} stopped containers")
            except Exception as e:
                self.logger.error(f"Failed to clean task definitions: {e}")
            await asyncio.sleep(300)

    def _create_subnet(self) -> str:
        ec2_client = boto3.client(
            "ec2",
            region_name=self.default_region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        ec2_resource = boto3.resource(
            "ec2",
            region_name=self.default_region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # check if default vpc exists with tag netunicorn:default
        vpcs = ec2_client.describe_vpcs(
            Filters=[
                {
                    "Name": "tag:netunicorn",
                    "Values": [
                        "default",
                    ],
                },
            ]
        )
        if vpcs["Vpcs"]:
            vpc_id = vpcs["Vpcs"][0]["VpcId"]
            vpc = ec2_resource.Vpc(vpc_id)
            for subnet in vpc.subnets.all():
                self.logger.info(
                    f"Default VPC {vpc_id} with tag netunicorn:default found,"
                    f" taking the first subnet with ID {subnet.id}"
                )
                return subnet.id

        self.logger.info("No default VPC found, creating new...")
        create_vpc_response = ec2_client.create_vpc(CidrBlock="10.113.0.0/16")
        vpc = ec2_resource.Vpc(create_vpc_response["Vpc"]["VpcId"])
        vpc.wait_until_available()
        vpc.create_tags(Tags=[{"Key": "netunicorn", "Value": "default"}])
        subnet = vpc.create_subnet(CidrBlock="10.113.0.0/16")


        create_ig_response = ec2_client.create_internet_gateway()
        ig_id = create_ig_response["InternetGateway"]["InternetGatewayId"]
        vpc.attach_internet_gateway(InternetGatewayId=ig_id)

        for route_table in vpc.route_tables.all():
            route_table.create_route(DestinationCidrBlock="0.0.0.0/0", GatewayId=ig_id)

        self.logger.info(
            f"Created VPC {vpc.id} with subnet {subnet.id}, "
            f"internet gateway {ig_id} and default route to the internet"
        )
        return subnet.id

    def _create_node_template(self, configuration: Optional[list[dict]]) -> None:
        if not configuration:
            self.logger.warning("No node configuration provided, using default AMD64 Linux node with 1 vCPU and 2GB RAM")
            self.node_template = [
                Node(
                    name=f"aws-fargate-default-",
                    properties={
                        "cpu": 1024,
                        "memory": 2048,
                    },
                    architecture=Architecture.LINUX_AMD64,
                )
            ]
            return

        self.logger.info(f"Creating node template with {len(configuration)} nodes")
        self.node_template = [
            Node(
                name=configuration[i].get("name", f"aws-fargate-{i}-"),
                properties=deepcopy(configuration[i].get("properties", {})) | {
                    "netunicorn-environments": {"DockerImage"},
                },
                architecture=(
                    Architecture.LINUX_ARM64
                    if configuration[i]
                    .get("properties", {})
                    .get("architecture", "amd64")
                    == "arm64"
                    else Architecture.LINUX_AMD64
                ),
            )
            for i, node in enumerate(configuration)
        ]

    def _verify_node_definition(self, node: Node) -> bool:
        if self.allow_custom_containers:
            return True

        for node_template in self.node_template:
            if node.name.startswith(node_template.name):
                return node.properties.get("cpu", 0) == node_template.properties.get(
                    "cpu", 0
                ) and node.properties.get("memory", 0) == node_template.properties.get(
                    "memory", 0
                )
        return False

    async def initialize(self, *args: Any, **kwargs: Any) -> None:
        self.cleaner_task = asyncio.create_task(self.__periodic_cleaner())

    async def health(self, *args: Any, **kwargs: Any) -> Tuple[bool, str]:
        try:
            clusters = self.__get_cluster_names()
            if self.cluster not in clusters:
                return False, f"Cluster {self.cluster} is not found"
            return True, ""
        except Exception as e:
            self.logger.exception(e)
            return False, str(e)

    async def shutdown(self, *args: Any, **kwargs: Any) -> None:
        if self.cleaner_task:
            self.cleaner_task.cancel()
        self.ecs_client.close()
        self.logger.info("AWS Fargate executor shutdown")

    async def get_nodes(
        self,
        username: str,
        authentication_context: Optional[dict[str, str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Nodes:
        node_template = deepcopy(self.node_template)
        for node in node_template:
            # making node names unique to prevent locks
            node.name += f"{username}-"
        nodes = UncountableNodePool(node_template=node_template, soft_limit=self.soft_limit)
        return nodes

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
        result: dict[str, Result[None, str]] = {}
        for deployment in deployments:
            result[deployment.executor_id] = (
                Success(None)
                .bind(
                    lambda x: Success(x)
                    if self._verify_node_definition(deployment.node)
                    else Failure(
                        "Node definition is invalid, custom node definitions are prohibited."
                    )
                )
                .bind(
                    lambda x: Success(x)
                    if deployment.prepared
                    else Failure("Deployment is not prepared")
                )
                .bind(
                    lambda x: Success(x)
                    if isinstance(deployment.environment_definition, DockerImage)
                    else Failure("AWS Fargate only supports DockerImage deployments")
                )
            )
        return result

    def _create_task_definition(
        self, experiment_id: str, deployment: Deployment
    ) -> Result[Deployment, str]:
        try:
            deployment.environment_definition.runtime_context.environment_variables[
                "NETUNICORN_EXECUTOR_ID"
            ] = deployment.executor_id
            deployment.environment_definition.runtime_context.environment_variables[
                "NETUNICORN_GATEWAY_ENDPOINT"
            ] = self.netunicorn_gateway
            deployment.environment_definition.runtime_context.environment_variables[
                "NETUNICORN_EXPERIMENT_ID"
            ] = experiment_id

            container_def = {
                "name": deployment.executor_id,
                "essential": True,
                "image": deployment.environment_definition.image,
                "environment": [
                    {"name": key, "value": value}
                    for key, value in deployment.environment_definition.runtime_context.environment_variables.items()
                ],
            }

            parameters = {
                "family": f"experiment-{experiment_id}",
                "networkMode": "awsvpc",
                "containerDefinitions": [container_def],
                "runtimePlatform": {
                    "cpuArchitecture": (
                        "ARM64"
                        if deployment.node.architecture == Architecture.LINUX_ARM64
                        else "X86_64"
                    ),
                    "operatingSystemFamily": "LINUX",
                },
                "requiresCompatibilities": ["FARGATE"],
            }
            if "cpu" in deployment.node.properties:
                parameters["cpu"] = str(deployment.node.properties.get("cpu", 256))
            if "memory" in deployment.node.properties:
                parameters["memory"] = str(
                    deployment.node.properties.get("memory", 512)
                )

            response = self.ecs_client.register_task_definition(**parameters)
            self.logger.debug(
                f"Created task definition {response['taskDefinition']['taskDefinitionArn']}"
                f" for deployment {deployment.executor_id}"
            )
            return Success(
                (
                    deployment,
                    experiment_id,
                    response["taskDefinition"]["taskDefinitionArn"],
                )
            )
        except Exception as e:
            self.logger.exception(e)
            return Failure(str(e))

    def _run_task(self, data: tuple[Deployment, str, str]) -> Result[None, str]:
        try:
            deployment, experiment_id, task_definition_arn = data
            response = self.ecs_client.run_task(
                cluster=self.cluster,
                taskDefinition=task_definition_arn,
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": [self.subnet],
                        "assignPublicIp": "ENABLED",
                    }
                },
                tags=[
                    {"key": "netunicorn-experiment-id", "value": experiment_id},
                    {"key": "netunicorn-executor-id", "value": deployment.executor_id},
                ],
            )
            if response["failures"]:
                self.logger.warning(f"Failed to run task: {response['failures']}")
                return Failure(str(response["failures"]))

            self.ecs_client.deregister_task_definition(
                taskDefinition=task_definition_arn
            )
            self.logger.debug(f"Successfully ran task for deployment {deployment.executor_id}")
            return Success(None)
        except Exception as e:
            self.logger.exception(e)
            return Failure(str(e))

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
        result: dict[str, Result] = {
            deployment.executor_id: Success(deployment) for deployment in deployments
        }

        # step 0: check for non-prepared deployments
        result = {
            executor_id: deployment_result.bind(
                lambda x: Failure("Deployment is not prepared")
                if not x.prepared
                else Success(x)
            )
            for executor_id, deployment_result in result.items()
        }

        # step 1: create task definition for each deployment
        result = {
            executor_id: deployment_result.bind(
                lambda x: self._create_task_definition(experiment_id, x)
            )
            for executor_id, deployment_result in result.items()
        }

        # step 2: run task for each deployment
        result = {
            executor_id: deployment_result.bind(self._run_task)
            for executor_id, deployment_result in result.items()
        }

        self.logger.debug(
            f"Started experiment {experiment_id} with "
            f"{sum(is_successful(x) for x in result.values())} deployments"
        )
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
        executor_ids = {request["executor_id"] for request in requests_list}
        results = {request["executor_id"]: None for request in requests_list}
        try:
            # get all tasks and their tags
            tasks = self.ecs_client.list_tasks(
                cluster=self.cluster,
            )
            for task in tasks["taskArns"]:
                tags_response = self.ecs_client.list_tags_for_resource(
                    resourceArn=task,
                )

                for tag in tags_response["tags"]:
                    if (
                        tag["key"] == "netunicorn-executor-id"
                        and tag["value"] in executor_ids
                    ):
                        self.ecs_client.stop_task(
                            cluster=self.cluster,
                            task=task,
                        )
                        self.logger.debug(f"Stopped task for executor {tag['value']}")
                        results[tag["value"]] = Success(None)
                        break

        except Exception as e:
            self.logger.exception(e)
            for request in requests_list:
                if results[request["executor_id"]] is None:
                    results[request["executor_id"]] = Failure(str(e))

        return results

    async def cleanup(
        self,
        experiment_id: str,
        deployments: list[Deployment],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        try:
            definitions = self.ecs_client.list_task_definitions(
                familyPrefix=f"experiment-{experiment_id}",
            )
            for definition in definitions["taskDefinitionArns"]:
                self.ecs_client.deregister_task_definition(
                    taskDefinition=definition,
                )
            if definitions["taskDefinitionArns"]:
                self.ecs_client.delete_task_definitions(
                    taskDefinitions=definitions["taskDefinitionArns"]
                )
        except Exception as e:
            self.logger.exception(e)


async def test():
    connector = AWSFargate(
        connector_name="test",
        configuration="configuration-example.yaml",
        netunicorn_gateway="http://localhost:8000",
    )

    await connector.initialize()
    await connector.health()


if __name__ == "__main__":
    asyncio.run(test())
