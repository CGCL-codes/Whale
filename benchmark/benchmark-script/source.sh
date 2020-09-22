#!/bin/bash
for val in {100..129}
do
	ssh node$val "hostname;source /etc/profile";
done