#!/usr/bin/env bash

source /etc/profile

if [ $1 == 'stop' ]; then
  if [ $# -ne 4 ]; then
      echo "USAGE: wash-data.sh stop <application id> <save_point_dir> <job id>"
  else
    application_id=$2
    save_point_dir=$3
    job_id=$4
    flink stop ${job_id} -p ${save_point_dir} -m yarn-cluster -yid ${application_id}
  fi
elif [ $1 == 'restart' ]; then
    if [ $# -ne 3 ]; then
      echo "USAGE: wash-data.sh restart <application id> <save_point_dir>"
    else
    application_id=$2
    save_point_dir=$3
    flink run -yid ${application_id} -s ${save_point_dir} \
    -c com.lhs.flink.jobs.comprehensive.JobMain wash-data-1.0-SNAPSHOT.jar \
    -kafka.consume.servers 10.122.238.97:9092 \
    -kafka.produce.servers 10.122.238.97:9092 \
    -kafka.partition.discover.interval.ms 30000 \
    -kafka.group wash_group \
    -config.sleep.ms 5000 \
    -metric.map.ttl 3600 \
    -kafka.sink.default.topic wash \
    -kafka.sink.error.topic error
  fi
elif [ $1 == 'start' ]; then
    flink run -m yarn-cluster -yjm 2048 -yn 4 -ys 2 -ytm 2048 -ynm wash_data -p 8 \
    -c com.lhs.flink.WashEntry wash-data-1.0-SNAPSHOT.jar \
    -kafka.consume.servers 10.122.238.97:9092 \
    -kafka.produce.servers 10.122.238.97:9092 \
    -kafka.partition.discover.interval.ms 30000 \
    -kafka.group wash_group \
    -config.sleep.ms 5000 \
    -metric.map.ttl 3600 \
    -kafka.sink.default.topic wash \
    -kafka.sink.error.topic error
fi