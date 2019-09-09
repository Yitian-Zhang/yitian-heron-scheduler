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
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * This is a WordCountTopology that does simple word counts.
 * <p>
 * In this WordCountTopology,
 * 1. the spout task generate a set of random words during initial "open" method.
 * (~128k words, 20 chars per word)
 * 2. During every "nextTuple" call, each spout simply picks a word at random and emits it
 * 3. Spouts use a fields grouping for their output, and each spout could send tuples to
 * every other bolt in the WordCountTopology
 * 4. Bolts maintain an in-memory map, which is keyed by the word emitted by the spouts,
 * and updates the count when it receives a tuple.
 */
public final class AuroraWordCountLatencyTopology {
    private AuroraWordCountLatencyTopology() {
    }

    /**
     * Main method
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 1) {
            throw new RuntimeException("Specify WordCountTopology name");
        }

        int parallelism = 2; // modified
        if (args.length > 1) {
            parallelism = Integer.parseInt(args[1]);
        }
        // 创建TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new WordSpout(), parallelism);
        builder.setBolt("consumer", new ConsumerBolt(), parallelism).fieldsGrouping("word", new Fields("word"));
//        builder.setBolt("acking", new AckingBolt(), 2).fieldsGrouping("consumer", new Fields("consumer"));

        // 创建Config对象
        Config conf = new Config();
        conf.setNumStmgrs(1); // default=parallelism

        // config可以配置的内容
        conf.setMaxSpoutPending(1000);
        conf.setMessageTimeoutSecs(10);
        conf.setDebug(true);
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

        // configure component resources
        conf.setComponentRam("word", ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));
        conf.setComponentRam("consumer", ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB));
//        conf.setComponentRam("acking", ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB)); // set ackingblot ram

        // configure container resources
        conf.setContainerDiskRequested(
                ExampleResources.getContainerDisk(2 * parallelism, parallelism)); // 4G -> 2
        conf.setContainerRamRequested(
                ExampleResources.getContainerRam(2 * parallelism, parallelism)); // 2G 在计算topology所需资源的时候，这里的container数量需要+1
        conf.setContainerCpuRequested(2); // default=2

        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    // Utils class to generate random String at given length
    public static class RandomString {
        private final char[] symbols;
        private final Random random = new Random();
        private final char[] buf;

        public RandomString(int length) {
            // Construct the symbol set
            StringBuilder tmp = new StringBuilder();
            for (char ch = '0'; ch <= '9'; ++ch) {
                tmp.append(ch);
            }
            for (char ch = 'a'; ch <= 'z'; ++ch) {
                tmp.append(ch);
            }
            symbols = tmp.toString().toCharArray();
            if (length < 1) {
                throw new IllegalArgumentException("length < 1: " + length);
            }
            buf = new char[length];
        }

        public String nextString() {
            for (int idx = 0; idx < buf.length; ++idx) {
                buf[idx] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }
    }

    /**
     * A spout that emits a random word
     */
    public static class WordSpout extends BaseRichSpout {
        private static final long serialVersionUID = 4322775001819135036L;
        private static final int ARRAY_LENGTH = 128 * 1024;
        private static final int WORD_LENGTH = 20;
        private final String[] words = new String[ARRAY_LENGTH];
        private final Random rnd = new Random(31);
        private SpoutOutputCollector collector;

        // deployed latency monitor ------------------------
        private long spoutStartTime;
        private int taskId;
        private Map<String,Long> startTimeMap;
        // -------------------------------------------------


        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        /**
         * 需要在spout的open和nextTuple方法中调用负载监视器，
         * 用于在一定的时间窗口内，收集各进程占用的CPU，内存和网络资源负载信息，以及各进程之间的数据流大小
         *
         * @param map
         * @param topologyContext
         * @param spoutOutputCollector
         */
        @Override
        @SuppressWarnings("rawtypes")
        public void open(Map map, TopologyContext topologyContext,
                         SpoutOutputCollector spoutOutputCollector) {
            RandomString randomString = new RandomString(WORD_LENGTH);
            for (int i = 0; i < ARRAY_LENGTH; i++) {
                words[i] = randomString.nextString();
            }
            collector = spoutOutputCollector;
            // deployed latency monitor ------------------------
            // get taskid from topologycontext
            taskId = topologyContext.getThisTaskId();
            startTimeMap = new HashMap<>();
            // -------------------------------------------------
        }

        // generate uuid for messageid----------------------
        public String generateUUID() {
            return UUID.randomUUID().toString().replace("-", "");
        }
        // -------------------------------------------------

        @Override
        public void nextTuple() {
            // default
//            int nextInt = rnd.nextInt(ARRAY_LENGTH);
//            collector.emit(new Values(words[nextInt]), generateUUID()); // acking

            // deployed latency monitor ------------------------
            spoutStartTime = System.currentTimeMillis(); // record spout start time
            // latency monitor using:
            int nextInt = rnd.nextInt(ARRAY_LENGTH);
            String uuid = generateUUID();
            String nextWord = words[nextInt];
            Values nextValue = new Values(nextWord, uuid);

            startTimeMap.put(uuid, spoutStartTime); // msgid->starttime
            collector.emit(nextValue, uuid);
            // -----------------------------------------------------------
        }



        /**
         * @param msgId
         */
        public void ack(Object msgId) {
            // deployed latency monitor ------------------------
            String messageId = (String) msgId; // message id
            long startTime = startTimeMap.get(messageId);
            startTimeMap.remove(messageId);
            long latency = System.currentTimeMillis() - startTime;
            LatencyMonitor.getInstance().setContent(String.valueOf(taskId), latency);
            // -------------------------------------------------
        }

        /**
         * @param msgId
         */
        public void fail(Object msgId) {
        }
    }

    /**
     * A bolt that counts the words that it receives
     */
    public static class ConsumerBolt extends BaseRichBolt {
        private static final long serialVersionUID = -5470591933906954522L;

        private OutputCollector collector;
        private Map<String, Integer> countMap;

        @SuppressWarnings("rawtypes")
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
            countMap = new HashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            String key = tuple.getString(0);
            if (countMap.get(key) == null) {
                countMap.put(key, 1);
            } else {
                Integer val = countMap.get(key);
                countMap.put(key, ++val);
            }
            // deployed latency monitor ------------------------
            collector.ack(tuple);
            // -------------------------------------------------
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }
}
