#!/bin/bash
for val in {24..28} {30..36} {42..45} {62..65} {88..90} {91..100}
do
	scp /whale/jars/communication/storm-client-2.0.0-SNAPSHOT.jar node$val:/whale/storm/apache-storm-2.0.0-SNAPSHOT/lib
	scp /whale/jars/communication/storm-client-2.0.0-SNAPSHOT.jar node$val:/whale/storm/apache-storm-2.0.0-SNAPSHOT/lib-worker
done	
