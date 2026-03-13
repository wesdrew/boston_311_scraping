#!/bin/bash

if uv run python infra/src/app.py; then
    echo "CDK synthesis successful!"
else
    echo "CDK failed! Check the errors above."
    exit 1
fi
