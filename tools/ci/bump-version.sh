#!/bin/bash

set -ex

if ([ "$TRAVIS_BRANCH" == "master" ] || [ ! -z "$TRAVIS_TAG" ]) && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
      git config --global user.email "travis@travis-ci.org"
      git config --global user.name "Travis CI"
      git remote set-url --push origin "https://${GH_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git"
      git remote -v
      git checkout -f -b version-branch

      pip install --upgrade bumpversion
      bumpversion patch

      git commit -m "$(git log -1 --pretty=%B) .... bump version [skip ci]"
      git push origin version-branch:master --follow-tags
else
      echo "version skiped!"
fi