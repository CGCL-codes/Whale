#!/bin/bash
source /etc/profile
nohup /whale/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm supervisor > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/supervisor.out 2>&1 &
nohup /wahle/storm/apache-storm-2.0.0-SNAPSHOT/bin/storm logviewer > /whale/storm/apache-storm-2.0.0-SNAPSHOT/logs/logviewer.out 2>&1 &
