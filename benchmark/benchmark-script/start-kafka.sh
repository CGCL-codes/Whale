#!/bin/sh
ssh node24 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"
ssh node25 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"
ssh node26 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"
ssh node27 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"
ssh node28 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"
ssh node30 "source /etc/profile;sh /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-server-start.sh -daemon /opt/kafka/kafka_2.10-0.10.2.0/config/server.properties"

