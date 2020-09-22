#nimbus节点

echo 从节点 node100 停止logviewer...[ done ]
kill -9 `ps -ef | grep daemon.logviewer | grep -v 'grep' | awk '{print $2}'| head -n 1` >/dev/null 2>&1

#停止所有的supervisor
for visor in $(seq 101 117)
do
    echo 从节点 node$visor 停止logviewer...[ done ]
    ssh node$visor "kill -9 `ssh node$visor ps -ef | grep daemon.logviewer | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
done

for visor in $(seq 119 129)
do
    echo 从节点 node$visor 停止logviewer...[ done ]
    ssh node$visor "kill -9 `ssh node$visor ps -ef | grep daemon.logviewer | grep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1
done
