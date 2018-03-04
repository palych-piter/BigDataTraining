#!/usr/bin/env bash

hadoop fs -rm -r /bgtraining/map-reduce/homework1/output
hadoop jar mapreduce-1.0.jar /bgtraining/map-reduce/homework1/input /bgtraining/map-reduce/homework1/output

