#!/bin/bash
#for val in {100..129}
for val in {2..3} 12 19 {23..28} 30 32 34 37 {61..62} {84..87} {90..93} 95 {97..98} {105..106} 108
do
	scp /whale/jars/basic/storm-client-2.0.0-SNAPSHOT.jar node$val:/whale/storm/apache-storm-2.0.0-SNAPSHOT/lib
	scp /whale/jars/basic/storm-client-2.0.0-SNAPSHOT.jar node$val:/whale/storm/apache-storm-2.0.0-SNAPSHOT/lib-worker
done	
