#!/bin/bash

set -e

git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis CI"
# git remote set-url --push origin "https://${GH_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git"
# git remote -v
git checkout -f -b version-branch

pip install --upgrade bump2version wheel
bump2version patch

git commit -am "$(git log -1 --pretty=%B) .... bump version [skip ci]"
git push origin version-branch:release_v2.4 --follow-tags

rm -rf ./dist
python setup.py sdist bdist_wheel

file_sha=$(curl -s --request GET \
  --url https://api.github.com/repos/kube-hpc/hkube/contents/core/algorithm-builder/environments/python/wrapper/requirements.txt?ref=release_v2.4 \
  --header 'accept: application/vnd.github.v3+json' \
  --header "authorization: token ${GH_TOKEN}" | jq -r .sha)
  echo $file_sha
release_v2_4_sha=$(curl -s --request GET \
  --url https://api.github.com/repos/kube-hpc/hkube/commits/release_v2.4 \
  --header 'accept: application/vnd.github.v3+json' \
  --header "authorization: token ${GH_TOKEN}" | jq -r .sha)
  echo $release_v2_4_sha
#version=$(python setup.py --version)
#branch_name="update_python_wrapper_to_${version//./_}"
#echo $branch_name
#curl --request POST \
#  --url https://api.github.com/repos/kube-hpc/hkube/git/refs \
#  --header 'accept: application/vnd.github.v3+json' \
#  --header "authorization: token ${GH_TOKEN}" \
#  --header 'content-type: application/json' \
#  --data '{
#      "sha": "'"$master_sha"'",
#      "ref":"refs/heads/'"$branch_name"'"
#  }'
#content=$(echo "hkube-python-wrapper==$version" | base64)
#curl --request PUT \
#  --url https://api.github.com/repos/kube-hpc/hkube/contents/core/algorithm-builder/environments/python/wrapper/requirements.txt \
#  --header 'accept: application/vnd.github.v3+json' \
#  --header "authorization: token ${GH_TOKEN}" \
#  --data '{
#      "message":"update python wrapper in builder to version '"$version"'",
#      "content": "'"$content"'",
#      "branch": "'"$branch_name"'",
#      "sha": "'"$file_sha"'"
#  }'
#
#  curl --request POST \
#  --url https://api.github.com/repos/kube-hpc/hkube/pulls \
#  --header 'accept: application/vnd.github.v3+json' \
#  --header "authorization: token ${GH_TOKEN}" \
#  --data '{
#      "title":"update python wrapper in builder to version '"$version"'",
#      "head": "'"$branch_name"'",
#      "base": "master"
#        }'