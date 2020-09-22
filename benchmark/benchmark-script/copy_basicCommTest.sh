#!/bin/bash
for val in {100..129}
do
	scp /storm/jars/basicCommTest/storm-client-2.0.0-SNAPSHOT.jar node$val:/storm/apache-storm-2.0.0-SNAPSHOT/lib
	scp /storm/jars/basicCommTest/storm-client-2.0.0-SNAPSHOT.jar node$val:/storm/apache-storm-2.0.0-SNAPSHOT/lib-worker
done	
