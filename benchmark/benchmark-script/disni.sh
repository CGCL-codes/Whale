#!/bin/bash
for val in {24..28} {30..36} {42..45} {62..65} {88..100}
do
	ssh node$val "hostname"
	ssh node$val "/whale/DISNI/disni/libdisni/autoprepare.sh;"
	ssh node$val "cd /whale/DISNI/disni/libdisni; ./configure --with-jdk=/opt/java/jdk1.8.0_151; make && make install"
done
