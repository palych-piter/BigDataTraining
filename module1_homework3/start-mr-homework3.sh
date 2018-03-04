#!/usr/bin/env bash

hadoop fs -rm -r /bgtraining/map-reduce/homework2/output
hadoop jar mapreduce-1.0.jar /bgtraining/map-reduce/homework3/input /bgtraining/map-reduce/homework3/output
