#!/bin/bash
for val in {100..129}
do
	ssh node$val "hostname;tar -zxvf /storm/jdk-8u151-linux-x64.tar.gz -C /storm/";
done	
