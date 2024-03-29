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

package heron.example.topology;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * This is a basic example of a Heron topology with acking enable.
 */
public final class AckingTopology {

    private AckingTopology() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Specify topology name");
        }
        TopologyBuilder builder = new TopologyBuilder();

        int spouts = 2;
        int bolts = 2;
        builder.setSpout("word", new AckingTestWordSpout(), spouts);
        builder.setBolt("exclaim1", new ExclamationBolt(), bolts)
                .shuffleGrouping("word");

        Config conf = new Config();
        conf.setDebug(true);

        // Specifies that all tuples will be automatically failed if not acked within 10 seconds
        conf.setMessageTimeoutSecs(10);

        // Put an arbitrarily large number here if you don't want to slow the topology down
        conf.setMaxSpoutPending(1000 * 1000 * 1000);

        // To enable at-least-once delivery semantics
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

        // Extra JVM options
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

        // Component resource configuration
        conf.setComponentRam("word", ExampleResources.getComponentRam());
        conf.setComponentRam("exclaim1", ExampleResources.getComponentRam());

        // Container resource configuration
        conf.setContainerDiskRequested(
                ExampleResources.getContainerDisk(spouts + bolts, 2));
        conf.setContainerRamRequested(
                ExampleResources.getContainerRam(spouts + bolts, 2));
        conf.setContainerCpuRequested(1);

        // Set the number of workers or stream managers
        conf.setNumStmgrs(2);
        HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }

    public static class AckingTestWordSpout extends BaseRichSpout {

        private static final long serialVersionUID = -630307949908406294L;
        private SpoutOutputCollector collector;
        private String[] words;
        private Random rand;

        public AckingTestWordSpout() {
        }

        @SuppressWarnings("rawtypes")
        public void open(
                Map conf,
                TopologyContext context,
                SpoutOutputCollector acollector) {
            collector = acollector;
            words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
            rand = new Random();
        }

        public void close() {
        }

        public void nextTuple() {
            final String word = words[rand.nextInt(words.length)];

            // To enable acking, we need to emit each tuple with a MessageId, which is an Object.
            // Each new message emitted needs to be annotated with a unique ID, which allows
            // the spout to keep track of which messages should be acked back to the producer or
            // retried when the appropriate ack/fail happens. For the sake of simplicity here,
            // however, we'll tag all tuples with the same message ID.
            collector.emit(new Values(word), "MESSAGE_ID");
        }

        // Specifies what happens when an ack is received from downstream bolts
        public void ack(Object msgId) {
        }

        // Specifies what happens when a tuple is failed by a downstream bolt
        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class ExclamationBolt extends BaseRichBolt {
        private static final long serialVersionUID = -2267338658317778214L;
        private OutputCollector collector;
        private long nItems;
        private long startTime;

        @Override
        @SuppressWarnings("rawtypes")
        public void prepare(Map conf, TopologyContext context, OutputCollector acollector) {
            collector = acollector;
            nItems = 0;
            startTime = System.currentTimeMillis();
        }

        @Override
        public void execute(Tuple tuple) {
            // We need to ack a tuple when we deem that some operation has successfully completed.
            // Tuples can also be failed by invoking collector.fail(tuple)
            // If we do not explicitly ack or fail the tuple after MessageTimeout seconds, which
            // can be set in the topology config, the spout will automatically fail the tuple
            ++nItems;
            if (nItems % 10000 == 0) {
                long latency = System.currentTimeMillis() - startTime;
                System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
                GlobalMetrics.incr("selected_items");
                // Here we explicitly forget to do the ack or fail
                // It would trigger fail on this tuple on spout end after MessageTimeout Seconds
            } else if (nItems % 2 == 0) {
                collector.fail(tuple);
            } else {
                collector.ack(tuple);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
