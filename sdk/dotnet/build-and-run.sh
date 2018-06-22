#!/bin/bash
set -eou pipefail
IFS="\n\t"

GOBIN=/opt/pulumi/bin go install ./cmd/pulumi-language-dotnet
export PATH=/opt/pulumi/bin:$PATH
cd examples/bucket
pulumi preview --diff
pulumi update --skip-preview --yes --diff
pulumi preview --diff
pulumi destroy --yes

