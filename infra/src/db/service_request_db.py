from aws_cdk import RemovalPolicy, Stack, aws_ec2, aws_rds
from constructs import Construct
from shared.network_stack import NetworkStack
from shared.secret_stack import SecretStack


class ServiceRequestDbStack(Stack):
    @classmethod
    def prod(
        cls,
        scope: Construct,
        construct_id: str,
        network: NetworkStack,
        secret_stack: SecretStack,
        **kwargs: dict,
    ) -> "ServiceRequestDbStack":
        return cls(
            scope,
            construct_id,
            network=network,
            secret_stack=secret_stack,
            removal_policy=RemovalPolicy.RETAIN,
            deletion_protection=True,
            **kwargs,
        )

    @classmethod
    def dev(
        cls,
        scope: Construct,
        construct_id: str,
        network: NetworkStack,
        secret_stack: SecretStack,
        **kwargs: dict,
    ) -> "ServiceRequestDbStack":
        return cls(
            scope,
            construct_id,
            network=network,
            secret_stack=secret_stack,
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            **kwargs,
        )

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        network: NetworkStack,
        secret_stack: SecretStack,
        *,
        removal_policy: RemovalPolicy,
        deletion_protection: bool,
        **kwargs: dict,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        subnet_group = aws_rds.SubnetGroup(
            self,
            "SubnetGroup",
            vpc=network.vpc,
            description="Subnet group for service_request RDS instance",
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PRIVATE_ISOLATED),
            removal_policy=removal_policy,
        )

        self.instance = aws_rds.DatabaseInstance(
            self,
            "Instance",
            engine=aws_rds.DatabaseInstanceEngine.mysql(version=aws_rds.MysqlEngineVersion.VER_8_0),
            instance_type=aws_ec2.InstanceType.of(aws_ec2.InstanceClass.T3, aws_ec2.InstanceSize.MICRO),
            vpc=network.vpc,
            subnet_group=subnet_group,
            security_groups=[network.rds_sg],
            credentials=aws_rds.Credentials.from_secret(secret_stack.secret),
            multi_az=False,
            database_name="boston311",
            removal_policy=removal_policy,
            deletion_protection=deletion_protection,
        )
