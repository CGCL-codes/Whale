/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 * storm jar storm-starter-2.0.0-SNAPSHOT.jar org.apache.storm.starter.WordCountTopology wordcountTopology
 */
public class WordCountTopology extends ConfigurableTopology {
  public static class SplitSentence extends BaseBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(SplitSentence.class);
    private int index=0;
    public SplitSentence() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      index++;
      String sentences = input.getStringByField("word");
      LOG.info("sentences : {} index: {}",sentences,index);
      //collector.emit(new Values(sentences));
      //for(String str :sentences.split(" ")){
        //collector.emit(new Values(str));
      //}
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(WordCount.class);

    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      LOG.info("word :{}, count :{}",word,count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {
      ConfigurableTopology.start(new WordCountTopology(), args);
  }

  protected int run(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 1);

    builder.setBolt("split", new SplitSentence(), 9).allGrouping("spout");
    //builder.setBolt("split", new SplitSentenceForCountBolt(), 3).allGrouping("spout");
    //builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //conf.setDebug(true);

    String topologyName = "word-count";

    conf.setNumWorkers(4);

    if (args != null && args.length > 0) {
      topologyName = args[0];
    }
    return submit(topologyName, conf, builder);
  }
}
