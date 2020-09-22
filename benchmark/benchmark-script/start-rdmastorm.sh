#!/bin/bash
#start zookeeper
#zkServer.sh start
#for i in $(seq 4 5);do ssh root$i "hostname;/home/tj/softwares/zookeeper-3.4.6/bin/zkServer.sh start";done
source /etc/profile
nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm nimbus > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/nimbus.out 2>&1 &
nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm ui > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/ui.out 2>&1 &
nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm logviewer > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/logviewer.out 2>&1 &

#for i in {2..3} {17..19} {24..28} 30 {32..34} 36 {42..45} {62..63} {90..95} {97..98}
#for i in {2..3} 18 {24..28} 30 32 34 {62..63} {90..93} 95 {97..98}
for i in 2 3 19 {24..28} 30 32 34 37 {61..62} {84..87} 90 92 93 95 98 108
#for i in {25..27}
do
    ssh node$i "hostname"
    ssh node$i "source /etc/profile;nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm supervisor > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/supervisor.out 2>&1 &"
    ssh node$i "source /etc/profile;nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm logviewer > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/logviewer.out 2>&1 &"
done
