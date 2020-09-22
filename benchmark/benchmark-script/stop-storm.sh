#nimbus相关进程: 
kill `ps aux | egrep '(daemon\.nimbus)|(storm\.ui\.core)' | fgrep -v egrep | awk '{print $2}'` 
#zkServer.sh stop
#kill supervisor相关进程

for i in $(seq 101 129); do
    ssh node$i "hostname;kill `ps aux | fgrep storm | fgrep -v 'fgrep' | awk '{print $2}'`";
    ssh node$i "kill -9 `ps -ef | grep LogviewerServer |fgrep -v 'grep' | awk '{print $2}'| head -n 1`" >/dev/null 2>&1  
    ssh node$i "kill -9 `ps -ef | grep supervisor | fgrep -v 'grep' |  awk '{print $2}'| head -n 1`" >/dev/null 2>&1
    done
#kill zookeeper相关进程
#for i in $(seq 4 5); do ssh root$i "hostname;/home/tj/softwares/zookeeper-3.4.6/bin/zkServer.sh stop";done
