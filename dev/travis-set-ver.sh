#!/bin/bash


export CI_PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version $@ 2>/dev/null | grep -v "INFO" | tail -n 1)
export CI_VERSION="${CI_PROJECT_VERSION%%-*}"
export CI_BASE_VERSION="${CI_VERSION%.*}"

export CI_COMMIT_HASH=${TRAVIS_COMMIT?"TRAVIS_COMMIT undefined. Are we running under travis-ci?"}
export CI_COMMIT_HASH_SHORT=$(git rev-parse --short ${CI_COMMIT_HASH})
export CI_TAG=${TRAVIS_TAG}

export CI_PYTHON_TAG=".dev${TRAVIS_BUILD_NUMBER}"