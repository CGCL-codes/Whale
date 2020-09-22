#!/bin/bash
index=0
#for val in 2 3 19 {24..28} 30 32 34 37 {61..62} {84..87} 90 92 93 95 98 108
for val in {2..3} 12 19 {23..28} 30 32 34 37 {61..62} {84..87} {90..93} 95 {97..98} {105..106} 108
do
	scp -r /whale/conf/storm.yaml node$val:/whale/storm/apache-storm-2.0.0-SNAPSHOT/conf/
	ssh node$val "sed -i 's/\"special-supervisor\"/\"special-supervisor$index\"/g'	/whale/storm/apache-storm-2.0.0-SNAPSHOT/conf/storm.yaml"
	index=$(($index+1)) 
done	
