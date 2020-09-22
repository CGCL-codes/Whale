#!/bin/bash
for val in {100..129}
do
	scp /storm/jars/master/storm-client-2.0.0-SNAPSHOT.jar node$val:/storm/jars/master/storm-client-2.0.0-SNAPSHOT.jar
done	