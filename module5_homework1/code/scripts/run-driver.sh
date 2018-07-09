#!/usr/bin/env bash

driver_cores=2
driver_memory=16G
num_executors=35
executor_cores=3
executor_memory=8G

spark-submit \
`# Resources` \
--conf spark.driver.cores=${driver_cores} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--queue default \
--total-executor-cores $((${num_executors}*${executor_cores} + ${driver_cores})) \
`# Entry Point and Mode` \
--conf spark.yarn.submit.waitAppCompletion=true \
--master yarn --deploy-mode cluster spark-core-1.0.jar \


echo "All is done"
