#!/bin/bash
for val in {24..28} {30..36} {42..45} {62..65} {88..90} {91..99}
do
	ssh node$val "hostname;mkdir -p /storm/apache-storm-2.0.0-SNAPSHOT/status/logs/";
done	
