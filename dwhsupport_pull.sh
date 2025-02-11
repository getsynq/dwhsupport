#!/usr/bin/env sh

git remote add dwhsupport git@github.com:getsynq/dwhsupport.git
git fetch dwhsupport main
git subtree pull --prefix dwhsupport dwhsupport main --squash