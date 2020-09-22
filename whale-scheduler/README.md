# storm-scheduler
Storm自定义实现直接分配调度器,代码修改自Twitter Storm核心贡献者徐明明,[此处为链接](https://github.com/xumingming/storm-lib/blob/master/src/jvm/storm/DemoScheduler.java).

### 开发背景
在准备开发Storm自定义之前,事先已经了解了下现有Storm使用的调度器,默认是DefaultScheduler,调度原理大体如下:
* 在新的调度开始之前,先扫描一遍集群,如果有未释放掉的slot,则先进行释放
* 然后优先选择supervisor节点中有空闲的slot,进行分配,以达到最终平均分配资源的目标

### 现有scheduler的不足之处
上述的调度器基本可以满足一般要求,但是针对下面个例还是无法满足:
* 让spout分配到固定的机器上去,因为所需的数据就在那上面
* 不想让2个Topology运行在同一机器上,因为这2个Topology都很耗CPU

### DirectScheduler的作用
DirectScheduler把划分单位缩小到组件级别,1个Spout和1个Bolt可以指定到某个节点上运行,如果没有指定,还是按照系统自带的调度器进行调度.这个配置在Topology提交的Conf配置中可配.

### 使用方法
#### 集群配置
* 打包此项目,将jar包拷贝到STORM_HOME/lib目录下,在nimbus节点上的Storm包
* 在nimbus节点的storm.yaml配置中,进行如下的配置:

    ```
    storm.scheduler: "org.apache.storm.scheduler.DirectScheduler"
    ```
* 然后是在supervisor的节点中进行名称的配置,配置项如下:
 
     ```
    supervisor.scheduler.meta:
    name: "your-supervisor-name"
    ```

在集群这部分的配置就结束了,然后重启nimbus,supervisor节点即可,集群配置只要1次配置即可.

#### 拓扑逻辑配置
见下面的代码设置,主要是把组件名和节点名称作为映射值传入
<pre><code>
    int numOfParallel;
    TopologyBuilder builder;
    StormTopology stormTopology;
    Config config;
    //待分配的组件名称与节点名称的映射关系
    HashMap<String, String> component2Node;

    //任务并行化数设为10个
    numOfParallel = 2;

    builder = new TopologyBuilder();

    String desSpout = "my_spout";
    String desBolt = "my_bolt";

    //设置spout数据源
    builder.setSpout(desSpout, new TestSpout(), numOfParallel);

    builder.setBolt(desBolt, new TestBolt(), numOfParallel)
                .shuffleGrouping(desSpout);

    config = new Config();
    config.setNumWorkers(numOfParallel);
    config.setMaxSpoutPending(65536);
    config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 40000);
    config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 40000);

    component2Node = new HashMap<>();

    component2Node.put(desSpout, "special-supervisor1");
    component2Node.put(desBolt, "special-supervisor2");

    //此标识代表topology需要被调度
    config.put("assigned_flag", "1");
    //具体的组件节点对信息
    config.put("design_map", component2Node);
        
    StormSubmitter.submitTopology("test", config, builder.createTopology());
</code></pre>
拓扑逻辑作业具体要被调度时,传入配置参数即可.

### 调度器后期优化
DirectScheduler只是针对原有的调度实现做了1层包装,后期可以进行更深层次的改造,涉及到节点在分配的时候slot的排序等等.




