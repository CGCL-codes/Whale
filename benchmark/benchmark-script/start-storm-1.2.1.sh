#!/bin/bash
#start zookeeper
#zkServer.sh start
#for i in $(seq 4 5);do ssh root$i "hostname;/home/tj/softwares/zookeeper-3.4.6/bin/zkServer.sh start";done
nohup /storm/apache-storm-1.2.1/bin/storm nimbus > /storm/apache-storm-1.2.1/logs/nimbus.out 2>&1 &
nohup /storm/apache-storm-1.2.1/bin/storm ui > /storm/apache-storm-1.2.1/logs/ui.out 2>&1 &
nohup /storm/apache-storm-1.2.1/bin/storm logviewer > /storm/apache-storm-1.2.1/logs/logviewer.out 2>&1 &
for i in $(seq  101 129);
do
	ssh node$i "hostname;source /etc/profile;nohup /storm/apache-storm-1.2.1/bin/storm supervisor > /storm/apache-storm-1.2.1/logs/supervisor.out 2>&1 &";
	ssh node$i "source /etc/profile;nohup /storm/apache-storm-1.2.1/bin/storm logviewer > /storm/apache-storm-1.2.1/logs/logviewer.out 2>&1 &";
done
