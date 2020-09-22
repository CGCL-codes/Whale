#!/bin/bash
for val in {101..129}
do
  scp /storm/apache-storm-2.0.0-SNAPSHOT/conf/storm.yaml node$val:/storm/apache-storm-2.0.0-SNAPSHOT/conf/storm.yaml
done
