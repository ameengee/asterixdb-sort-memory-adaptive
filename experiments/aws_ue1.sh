#!/usr/bin/env bash
# Guarded AWS wrapper -- FORCES us-east-1 and REFUSES anything mentioning us-west.
#
# Why this exists: this account's CLI default region is us-west-2, which carries the owner's
# production traffic. A bare `aws ...` with no --region flag would silently operate there.
# Every AWS call for these experiments must go through this wrapper.
#
# Usage:  ./aws_ue1.sh ec2 describe-instances --filters ...
set -euo pipefail

REGION="us-east-1"

# Refuse if any argument references a forbidden region, however it is spelled.
for arg in "$@"; do
  case "$arg" in
    *us-west*|*USW*|*usw1*|*usw2*)
      echo "REFUSED: argument mentions a forbidden region: $arg" >&2
      exit 99
      ;;
  esac
done

# Refuse an explicit --region that is not us-east-1.
prev=""
for arg in "$@"; do
  if [[ "$prev" == "--region" && "$arg" != "$REGION" ]]; then
    echo "REFUSED: --region $arg (only $REGION is permitted)" >&2
    exit 99
  fi
  prev="$arg"
done

# Env vars beat the ~/.aws/config default, so us-west-2 cannot leak in via configuration.
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

exec aws --region "$REGION" "$@"
