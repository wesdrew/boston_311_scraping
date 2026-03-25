import subprocess

from aws_cdk import App, Tags
from db.service_request_db import ServiceRequestDbStack
from polling.polling_stack import PollingStack
from shared.network_stack import NetworkStack
from shared.secret_stack import SecretStack

subprocess.run(
    ["uv", "export", "--frozen", "--package", "polling", "--no-emit-workspace", "-o", "polling/requirements.txt"],
    check=True,
)
subprocess.run(
    ["uv", "export", "--frozen", "--package", "shared", "--no-emit-workspace", "-o", "shared/requirements.txt"],
    check=True,
)

app: App = App()

Tags.of(app).add("project", "Boston311")

network_stack: NetworkStack = NetworkStack(app, "Boston311-Network")
secret_stack: SecretStack = SecretStack(app, "Boston311-SecretManager")
db_stack: ServiceRequestDbStack = ServiceRequestDbStack.prod(
    app, "Boston311-Db", network=network_stack, secret_stack=secret_stack
)
dev_db_stack: ServiceRequestDbStack = ServiceRequestDbStack.dev(
    app, "Boston311-Db-dev", network=network_stack, secret_stack=secret_stack
)
polling_stack: PollingStack = PollingStack(
    app, "Boston311", network=network_stack, db=db_stack, secret_stack=secret_stack
)
dev_polling_stack: PollingStack = PollingStack(
    app, "Boston311-dev", network=network_stack, db=dev_db_stack, secret_stack=secret_stack
)

app.synth()
