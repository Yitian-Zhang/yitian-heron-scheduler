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

package zyt.custom.topology.aurora.benchmark;

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
import zyt.custom.my.scheduler.LatencyMonitor;
import zyt.custom.my.scheduler.TaskMonitor;
import zyt.custom.my.scheduler.WorkerMonitor;
import zyt.custom.tools.FileReader;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * ****************************************
 * add at 2018-07-06: running for test cluster that has 4 nodes
 * 2018-10-03: update for BW algorithm
 * ****************************************
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

        // benchmark for TEST_CLUSTAR(5 Nodes), the parallelism =
        // benchmark for FORMAL_CLUSTER(8 Nodes), the parallelism =
        builder.setSpout("spout", new FileReadSpout(), 4);
        builder.setBolt("split", new SplitSentence(), 10).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 10).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        // 2018-07-06 add for benchmark4**********************************************
        // maxspoutpending record: 1000->10000
        conf.setMaxSpoutPending(1000); // modified for latency
        conf.setMessageTimeoutSecs(60); // modified for latency
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE); // latency shows config
//        conf.setContainerRamPadding(ByteAmount.fromGigabytes(1)); // for RR. default=2G
        // ***************************************************************************

        // component resource configuration
        conf.setComponentRam("spout", ByteAmount.fromMegabytes(512));
        conf.setComponentRam("split", ByteAmount.fromMegabytes(512));
        conf.setComponentRam("count", ByteAmount.fromMegabytes(512)); // default: 512mb

        // container resource configuration
        conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3)); // default: 3g
        conf.setContainerRamRequested(ByteAmount.fromGigabytes(3)); // default: 3g
        conf.setContainerCpuRequested(2); // default: 2

        conf.setNumStmgrs(6);

        HeronSubmitter.submitTopology(name, conf, builder.createTopology());
    }


    /**
     * Read sentence from txt file
     * 2018-10-11 add
     */
    public static class FileReadSpout extends BaseRichSpout {
        private static final long serialVersionUID = -3423680327992254990L;
        private static final String TXT_FILE = "/A_Tale_of_Two_City.txt";
        private SpoutOutputCollector collector;
        private static FileReader fileReader = null;

        // deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------
        // deployed latency monitor ------------------------
        private long spoutStartTime;
        private int taskId;
        private Map<String, Long> startTimeMap;
        // -------------------------------------------------

        @Override
        public void open(Map<String, Object> conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            // deployed load monitor -------------------------------------------------------
            WorkerMonitor.getInstance().setContextInfo(topologyContext); // start thread and set topology (check topology)
            taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
            // -----------------------------------------------------------------------------

            collector = spoutOutputCollector;
            // file reader ---------------------------
            fileReader = new FileReader(TXT_FILE);
            // ---------------------------------------

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
            // deployed load monitor -------------------------------------------------------
            taskMonitor.checkThreadId();
            // -----------------------------------------------------------------------------

            // deployed latency monitor ------------------------
            spoutStartTime = System.currentTimeMillis(); // record spout start time
            // latency monitor using:
            String uuid = generateUUID();
            Values nextValue = new Values(fileReader.nextLine(), uuid);
            startTimeMap.put(uuid, spoutStartTime); // msgid->starttime
            collector.emit(nextValue, uuid);
            // -----------------------------------------------------------

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
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
            super.fail(msgId);
        }
    }

    /**
     * ****************************************
     * 2018-05-26
     * stay extends BaseBasicBolt un-changed
     * ****************************************
     */
    public static class SplitSentence extends BaseBasicBolt {
        private static final long serialVersionUID = 1249629174039601217L;

        // deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------

        @Override
        public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
            // deployed load monitor -------------------------------------------------------
            WorkerMonitor.getInstance().setContextInfo(topologyContext);
            taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
            // -----------------------------------------------------------------------------
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            // deployed load monitor -------------------------------------------------------
            taskMonitor.notifyTupleReceived(tuple);
            // -----------------------------------------------------------------------------
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

    /**
     * ************************************
     * 2018-05-26 udpate
     * modified BaseBasicBlot to BaseRichBolt to deployed latency monitor
     * ************************************
     */
    public static class WordCount extends BaseRichBolt {
        private static final long serialVersionUID = -8492566595062774310L;

        private Map<String, Integer> counts = new HashMap<String, Integer>();
        // deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------

        // modified baseRichBolt --------------------
        private OutputCollector collector;
        // ------------------------------------------

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
            // deployed load monitor -------------------------------------------------------
            WorkerMonitor.getInstance().setContextInfo(topologyContext);
            taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
            // -----------------------------------------------------------------------------
            // modified baseRichBolt --------------------
            collector = outputCollector;
            // ------------------------------------------
        }

        @Override
        public void execute(Tuple tuple) {
            // deployed load monitor -------------------------------------------------------
            taskMonitor.notifyTupleReceived(tuple);
            // -----------------------------------------------------------------------------

            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            // modified baseRichBolt --------------------
//            collector.emit(new Values(word, count));
            collector.ack(tuple);
            // ------------------------------------------
        }
    }
}
