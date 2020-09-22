#!/bin/sh
ssh node24 "source /etc/profile;/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh start"
ssh node25 "source /etc/profile;/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh start"
ssh node26 "source /etc/profile;/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh start"
