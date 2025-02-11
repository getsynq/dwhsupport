#!/usr/bin/env sh

set -e

 if [ -z "$*" ]; then echo "No args"; exit 1; fi

git remote add dwhsupport git@github.com:getsynq/dwhsupport.git || true
git fetch dwhsupport main
OUT_BRANCH="$1"
git subtree push --prefix=dwhsupport dwhsupport "$OUT_BRANCH"
