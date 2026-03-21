import subprocess

from aws_cdk import App, Tags
from polling.polling_stack import PollingStack

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

polling_stack: PollingStack = PollingStack(app, "Boston311")
dev_polling_stack: PollingStack = PollingStack(app, "Boston311-dev")

app.synth()
