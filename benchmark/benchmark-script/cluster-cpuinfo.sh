#!/bin/bash
for i in {24..28} {30..36} {42..45} {62..65} {88..90} {91..100};
do
	ssh node$i "hostname"
	ssh node$i "/whale/script/cpuinfo.sh"
done
