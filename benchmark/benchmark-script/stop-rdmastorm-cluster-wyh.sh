#nimbus节点
nimbusServers='node91'

#停止所有的nimbus和ui和log
for nim in $nimbusServers
do
    echo 从节点 $nim 停止nimbus和ui...[ done ]
    ssh $nim "kill -9 `ssh $nim ps -ef | grep nimbus | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
    ssh $nim "kill -9 `ssh $nim ps -ef | grep core | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
    ssh $nim "kill -9 `ssh $nim ps -ef | grep LogviewerServer | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
done

#停止所有的supervisor
#for visor in {2..3} {17..19} {24..28} 30 {32..34} 36 {42..45} {62..63} {90..95} {97..98}
#for visor in {2..3} 18 {24..28} 30 32 34 {62..63} {90..93} 95 {97..98}
for visor in 2 3 19 {24..28} 30 32 34 37 {61..62} {84..87} 90 92 93 95 98 108
#for visor in {25..27}
do
    echo 从节点 node$visor 停止supervisor...[ done ]
    ssh node$visor "kill -9 `ssh node$visor ps -ef | grep supervisor | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
    ssh node$visor "kill -9 `ssh node$visor ps -ef | grep LogviewerServer | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
done
