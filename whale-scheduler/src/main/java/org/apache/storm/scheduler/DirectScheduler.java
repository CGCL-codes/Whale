package org.apache.storm.scheduler;

import java.util.*;

/**
 * locate org.apache.storm.scheduler
 * Created by MasterTj on 2019/10/11.
 */
public class DirectScheduler implements IScheduler{

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("DirectScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        TopologyDetails topology;
        //作业是否要指定分配的标识
        String assignedFlag;
        Map map;
        Iterator<String> iterator = null;

        topologyDetailes = topologies.getTopologies();
        for(TopologyDetails td: topologyDetailes){
            map = td.getConf();
            assignedFlag = (String)map.get("assigned_flag");

            //如何找到的拓扑逻辑的分配标为1则代表是要分配的,否则走系统的调度
            if(assignedFlag != null && assignedFlag.equals("1")){
                System.out.println("finding topology named " + td.getName());
                topologyAssign(cluster, td, map);
            }else {
                System.out.println("topology assigned is null");
            }
        }

        //其余的任务由系统自带的调度器执行
        new EvenScheduler().schedule(topologies, cluster);
    }

    @Override
    public Map<String, Object> config() {
        return new HashMap<>();
    }


    /**
     * 拓扑逻辑的调度
     * @param cluster
     * 集群
     * @param topology
     * 具体要调度的拓扑逻辑
     * @param map
     * map配置项
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map){
        Set<String> keys;
        Map<String, Object> designMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {
            designMap = (Map<String, Object>) map.get("design_map");
            if(designMap != null){
                System.out.println("design map size is " + designMap.size());
                keys = designMap.keySet();
                iterator = keys.iterator();

                System.out.println("keys size is " + keys.size());
            }

            if(designMap == null || designMap.size() == 0){
                System.out.println("design map is null");
            }

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println("Our special topology does not need scheduling.");
            } else {
                System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }

                String componentName;
                String nodeName;
                if(designMap != null && iterator != null){
                    while (iterator.hasNext()){
                        componentName = iterator.next();
                        nodeName = (String)designMap.get(componentName);

                        System.out.println("现在进行调度 组件名称->节点名称:" + componentName + "->" + nodeName);
                        componentAssign(cluster, topology, componentToExecutors, componentName, nodeName);
                    }
                }
            }
        }
    }

    /**
     * 组件调度
     * @param cluster
     * 集群的信息
     * @param topology
     * 待调度的拓扑细节信息
     * @param totalExecutors
     * 组件的执行器
     * @param componentName
     * 组件的名称
     * @param supervisorName
     * 节点的名称
     */
    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors, String componentName, String supervisorName){
        if (!totalExecutors.containsKey(componentName)) {
            System.out.println("Our special-spout does not need scheduling.");
        } else {
            System.out.println("Our special-spout needs scheduling.");
            List<ExecutorDetails> executors = totalExecutors.get(componentName);

            // find out the our "special-supervisor" from the supervisor metadata
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            SupervisorDetails specialSupervisor = null;
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();

                if(meta != null && meta.get("name") != null){
                    System.out.println("supervisor name:" + meta.get("name"));

                    if (meta.get("name").equals(supervisorName)) {
                        System.out.println("Supervisor finding");
                        specialSupervisor = supervisor;
                        break;
                    }
                }else {
                    System.out.println("Supervisor meta null");
                }

            }

            // found the special supervisor
            if (specialSupervisor != null) {
                System.out.println("Found the special-supervisor");
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);

                // 如果目标节点上已经没有空闲的slot,则进行强制释放
                if (availableSlots.isEmpty() && !executors.isEmpty()) {
                    for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                        cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                    }
                }

                // 重新获取可用的slot
                availableSlots = cluster.getAvailableSlots(specialSupervisor);

                // 选取节点上第一个slot,进行分配
                cluster.assign(availableSlots.get(0), topology.getId(), executors);
                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
            } else {
                System.out.println("There is no supervisor find!!!");
            }
        }
    }
}
