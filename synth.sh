#!/bin/bash

echo "Generating requirements.txt for polling lambda"
uv export --frozen --package polling --no-emit-workspace -o polling/requirements.txt > /dev/null

echo "Generating requirements.txt for shared layer"
uv export --frozen --package shared --no-emit-workspace -o shared/requirements.txt > /dev/null

echo "Running CDK" 
if uv run python infra/src/app.py > /dev/null; then
    echo "CDK synthesis successful!"
else
    echo "CDK failed! Check the errors above."
    exit 1
fi

echo "Finished!" 