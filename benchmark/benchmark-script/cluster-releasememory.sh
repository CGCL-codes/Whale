#!/bin/sh
echo 1 > /proc/sys/vm/drop_caches

for i in {24..28} {30..36} {42..45} {62..65} {88..90} {91..100};
    do
        ssh node$i "echo 1 > /proc/sys/vm/drop_caches";
    done
