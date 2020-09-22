#!/bin/bash
index=0
for val in 3 19 {24..28} 30 32 34 37 {61..63} {84..87} {90..93} 95 {97..98} 108
do
	ssh node$val "rm -rf /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/*"
done	
