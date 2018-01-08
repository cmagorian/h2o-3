#!/usr/bin/env bash

git pull
export BUILD_HADOOP=true
export H2O_TARGET=hdp2.2
./gradlew build -x check -x :h2o-r:build -x :h2o-py:build
