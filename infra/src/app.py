from aws_cdk import App, Tags
from polling_stack import PollingStack

app: App = App()

Tags.of(app).add("project", "Boston311Polling")

polling_stack: PollingStack = PollingStack(app, "Boston311Polling")

app.synth()
