from aws_cdk import Stack, aws_ec2
from constructs import Construct


class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: dict) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = aws_ec2.Vpc(
            self,
            "Boston311Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                aws_ec2.SubnetConfiguration(
                    name="db",
                    subnet_type=aws_ec2.SubnetType.PRIVATE_ISOLATED,
                )
            ],
        )

        self.consumer_lambda_sg = aws_ec2.SecurityGroup(
            self,
            "ConsumerLambdaSg",
            vpc=self.vpc,
            description="Security group for consumer Lambda",
            allow_all_outbound=True,
        )

        self.rds_sg = aws_ec2.SecurityGroup(
            self,
            "RdsSg",
            vpc=self.vpc,
            description="Security group for RDS — inbound MySQL from consumer Lambda only",
            allow_all_outbound=False,
        )
        self.rds_sg.add_ingress_rule(
            peer=self.consumer_lambda_sg,
            connection=aws_ec2.Port.MYSQL_AURORA,
            description="MySQL from consumer Lambda",
        )

        private_subnets = aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PRIVATE_ISOLATED)

        self.vpc.add_interface_endpoint(
            "SqsEndpoint",
            service=aws_ec2.InterfaceVpcEndpointAwsService.SQS,
            subnets=private_subnets,
        )
        self.vpc.add_interface_endpoint(
            "SnsEndpoint",
            service=aws_ec2.InterfaceVpcEndpointAwsService.SNS,
            subnets=private_subnets,
        )
        self.vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=aws_ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            subnets=private_subnets,
        )
        self.vpc.add_interface_endpoint(
            "CloudWatchLogsEndpoint",
            service=aws_ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            subnets=private_subnets,
        )
