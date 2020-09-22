#!/bin/sh
ssh node24 "/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh stop"
ssh node25 "/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh stop"
ssh node26 "/opt/zookeeper/zookeeper-3.4.6/bin/zkServer.sh stop"
