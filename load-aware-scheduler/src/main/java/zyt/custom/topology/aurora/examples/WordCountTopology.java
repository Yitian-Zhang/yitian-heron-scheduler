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

import heron.example.topology.ExampleResources;
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
import zyt.custom.scheduler.monitor.TaskMonitor;
import zyt.custom.scheduler.monitor.WorkerMonitor;
import zyt.custom.topology.common.TopologyConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Topology Introduction
 *
 * This is a topology that does simple word counts. In this topology,
 * 1. the spout task generate a set of random words during initial "open" method.
 * (~128k words, 20 chars per word)
 * 2. During every "nextTuple" call, each spout simply picks a word at random and emits it
 * 3. Spouts use a fields grouping for their output, and each spout could send tuples to
 * every other bolt in the topology
 * 4. Bolts maintain an in-memory map, which is keyed by the word emitted by the spouts,
 * and updates the count when it receives a tuple.
 *
 * Updated for monitoring the latency and CPU load information.
 *
 * xxxx -------------------------------------------------------
 * In this content, there is the code to monitor some metrics.
 * Including:
 * 1. Task Monitor
 * 2. Latency Monitor
 * 3. Load Monitor
 * ------------------------------------------------------------
 *
 * 2018-09-12
 * @author yitian
 */
public final class WordCountTopology {

    private WordCountTopology() {
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        if (args.length < 1) {
            throw new RuntimeException("Specify topology name");
        }

        int parallelism = 1;
        if (args.length > 1) {
            parallelism = Integer.parseInt(args[1]);
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new WordSpout(), 4); // default parallelism
        builder.setBolt("consumer", new ConsumerBolt(), 7)
                .fieldsGrouping("word", new Fields("word")); // default parallelism
        Config conf = new Config();

        // default configure container resources
        /*
        conf.setContainerDiskRequested(
                ExampleResources.getContainerDisk(2 * parallelism, parallelism)); // 2G
        conf.setContainerRamRequested(
                ExampleResources.getContainerRam(2 * parallelism, parallelism)); // 1G
        conf.setContainerCpuRequested(2); // 2cores
         */

        // changed this value based on your experiment environment.
        conf.setNumStmgrs(3);

        // 2018-09-12 add for benchmark4
        conf.setMaxSpoutPending(TopologyConstants.EXAMPLE_WORDCOUNT_PENDING);
        conf.setMessageTimeoutSecs(TopologyConstants.EXAMPLE_WORDCOUNT_TIMEOUT);
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

        // configure component resources
        conf.setComponentRam("word",
                ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB)); // 512mb
        conf.setComponentRam("consumer",
                ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB)); // 512mb

        // this configuration is the max value of an container
        conf.setContainerDiskRequested(ByteAmount.fromGigabytes(TopologyConstants.EXAMPLE_CONTAINER_DISK_REQUESTED)); // 6G
        conf.setContainerRamRequested(ByteAmount.fromGigabytes(TopologyConstants.EXAMPLE_CONTAINER_RAM_REQUESTED)); // 4G
        conf.setContainerCpuRequested(TopologyConstants.EXAMPLE_CONTAINER_CPU_REQUESTED); // 4

        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    /**
     * Utils class to generate random String at given length
     */
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

        // Deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------
        // deployed latency monitor ------------------------
        private long spoutStartTime;
        private int taskId;
        private Map<String,Long> startTimeMap;
        // -------------------------------------------------

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void open(Map map, TopologyContext topologyContext,
                         SpoutOutputCollector spoutOutputCollector) {

            // deployed load monitor -------------------------------------------------------
            WorkerMonitor.getInstance().setContextInfo(topologyContext); // start thread and set topology (check topology)
            taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
            // -----------------------------------------------------------------------------

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

            // default content
            /*
            int nextInt = rnd.nextInt(ARRAY_LENGTH);
            collector.emit(new Values(words[nextInt]));
             */

            // deployed load monitor -------------------------------------------------------
            taskMonitor.checkThreadId();
            // -----------------------------------------------------------------------------
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

        @Override
        public void ack(Object msgId) {
            // deployed latency monitor ------------------------
            String messageId = (String) msgId; // message id
            long startTime = startTimeMap.get(messageId);
            startTimeMap.remove(messageId);

            // compute spout latency
            long latency = System.currentTimeMillis() - startTime;
            LatencyMonitor.getInstance().setContent(String.valueOf(taskId), latency);
            // -------------------------------------------------
        }

        @Override
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

        // deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------

        @SuppressWarnings("rawtypes")
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

            // deployed load monitor -------------------------------------------------------
            WorkerMonitor.getInstance().setContextInfo(topologyContext);
            taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
            // -----------------------------------------------------------------------------
            collector = outputCollector;
            countMap = new HashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            // deployed load monitor -------------------------------------------------------
            taskMonitor.notifyTupleReceived(tuple);
            // -----------------------------------------------------------------------------

            String key = tuple.getString(0);
            if (countMap.get(key) == null) {
                countMap.put(key, 1);
            } else {
                Integer val = countMap.get(key);
                countMap.put(key, ++val);
            }
            // -------------------------------
            collector.ack(tuple);
            // -------------------------------
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }
}
