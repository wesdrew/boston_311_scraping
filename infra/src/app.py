import subprocess

from aws_cdk import App, Tags
from polling_stack import PollingStack

subprocess.run(
    ["uv", "export", "--frozen", "--package", "polling", "--no-emit-workspace", "-o", "polling/requirements.txt"],
    check=True,
)
subprocess.run(
    ["uv", "export", "--frozen", "--package", "shared", "--no-emit-workspace", "-o", "shared/requirements.txt"],
    check=True,
)

app: App = App()

Tags.of(app).add("project", "Boston311Polling")

polling_stack: PollingStack = PollingStack(app, "Boston311Polling")

app.synth()
