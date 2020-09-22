#!/bin/bash
for val in {24..28} {30..36} {42..45} {62..65} {88..99}
do
	scp -r /opt/spark/spark-2.3.1-bin-hadoop2.7/conf/spark-env.sh node$val:/opt/spark/spark-2.3.1-bin-hadoop2.7/conf/
done	
