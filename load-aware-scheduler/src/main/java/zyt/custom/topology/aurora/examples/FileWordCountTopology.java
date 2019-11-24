//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package zyt.custom.topology.aurora.examples;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.ByteAmount;
import zyt.custom.scheduler.monitor.LatencyMonitor;
import zyt.custom.scheduler.monitor.TaskMonitor;
import zyt.custom.scheduler.monitor.WorkerMonitor;
import zyt.custom.topology.common.TopologyConstants;
import zyt.custom.utils.FileReader;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * FileWordCount without no any monitor
 *
 * @author yitian
 */
public final class FileWordCountTopology {

    private FileWordCountTopology() {
    }

    public static void main(String[] args) throws Exception {
        String name = "fast-word-count-topology";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FileReadSpout(), 4);
        builder.setBolt("split", new SplitSentence(), 10).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 10).fieldsGrouping("split",
                new Fields("word"));

        Config conf = new Config();
        conf.setMaxSpoutPending(1000); // modified for latency
        conf.setMessageTimeoutSecs(60); // modified for latency
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE); // latency shows config

        // component resource configuration
        conf.setComponentRam("spout", ByteAmount.fromMegabytes(TopologyConstants.BENCHMARK_COMPONENT_RAM));
        conf.setComponentRam("split", ByteAmount.fromMegabytes(TopologyConstants.BENCHMARK_COMPONENT_RAM));
        conf.setComponentRam("count", ByteAmount.fromMegabytes(TopologyConstants.BENCHMARK_COMPONENT_RAM)); // default: 512mb

        // container resource configuration
        conf.setContainerDiskRequested(ByteAmount.fromGigabytes(TopologyConstants.BENCHMARK_CONTAINER_DISK_REQUESTED)); // default: 3g
        conf.setContainerRamRequested(ByteAmount.fromGigabytes(TopologyConstants.BENCHMARK_CONTAINER_RAM_REQUESTED)); // default: 3g
        conf.setContainerCpuRequested(TopologyConstants.BENCHMARK_CONTAINER_CPU_REQUESTED); // default: 2

        conf.setNumStmgrs(TopologyConstants.BENCHMARK_STMGR_NUM);

        HeronSubmitter.submitTopology(name, conf, builder.createTopology());
    }


    /**
     * Read sentence from txt file
     */
    public static class FileReadSpout extends BaseRichSpout {
        private static final long serialVersionUID = -3423680327992254990L;
        private static final String TXT_FILE = "/A_Tale_of_Two_City.txt";
        private SpoutOutputCollector collector;
        private static FileReader fileReader = null;

        @Override
        public void open(Map<String, Object> conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            collector = spoutOutputCollector;
            fileReader = new FileReader(TXT_FILE);
        }

        public String generateUUID() {
            return UUID.randomUUID().toString().replace("-", "");
        }

        @Override
        public void nextTuple() {
            String uuid = generateUUID();
            Values nextValue = new Values(fileReader.nextLine(), uuid);
            collector.emit(nextValue, uuid);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }

        @Override
        public void ack(Object msgId) {
        }

        @Override
        public void fail(Object msgId) {
            super.fail(msgId);
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        private static final long serialVersionUID = 1249629174039601217L;

        @Override
        public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCount extends BaseRichBolt {
        private static final long serialVersionUID = -8492566595062774310L;

        private Map<String, Integer> counts = new HashMap<String, Integer>();

        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {

            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            collector.ack(tuple);
        }
    }
}
