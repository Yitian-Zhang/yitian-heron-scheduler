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
import com.twitter.heron.api.topology.IUpdatable;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.common.basics.ByteAmount;
import zyt.custom.scheduler.monitor.TaskMonitor;
import zyt.custom.scheduler.monitor.WorkerMonitor;
import zyt.custom.topology.aurora.spout.TestWordSpout;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public final class ExclamationTopology {

    private ExclamationTopology() {
    }

    public static void main(String[] args) throws Exception {
        String name = "fast-word-count-topology";
        TopologyBuilder builder = new TopologyBuilder();
        int parallelism = 2;

//        int spouts = parallelism;
        builder.setSpout("word", new TestWordSpout(Duration.ofMillis(0)), 8);
//        int bolts = 2 * parallelism;
        builder.setBolt("exclaim1", new ExclamationBolt(), 16)
                .shuffleGrouping("word");

        Config conf = new Config();
//        conf.setDebug(true);
//        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

        conf.setMaxSpoutPending(1000); // modified for latency
        conf.setMessageTimeoutSecs(60); // modified for latency
        conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE); // latency shows config

        // resources configuration
//        conf.setComponentRam("word", ExampleResources.getComponentRam());
//        conf.setComponentRam("exclaim1",
//                ExampleResources.getComponentRam());

//        conf.setContainerDiskRequested(
//                ExampleResources.getContainerDisk(spouts + bolts, parallelism));
//        conf.setContainerRamRequested(
//                ExampleResources.getContainerRam(spouts + bolts, parallelism));
//        conf.setContainerCpuRequested(1);

        // component resource configuration
        conf.setComponentRam("word", ByteAmount.fromMegabytes(512));
        conf.setComponentRam("exclaim1", ByteAmount.fromMegabytes(512)); // default: 512mb

        // container resource configuration
        conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3)); // default: 3g
        conf.setContainerRamRequested(ByteAmount.fromGigabytes(3)); // default: 3g
        conf.setContainerCpuRequested(2); // default: 2
        conf.setNumStmgrs(6);

        HeronSubmitter.submitTopology(name, conf, builder.createTopology());


    }

    public static class ExclamationBolt extends BaseRichBolt implements IUpdatable {

        private static final long serialVersionUID = 1184860508880121352L;
        private long nItems;
        private long startTime;

        // deployed load monitor -------------------------------------------------------
        private TaskMonitor taskMonitor;
        // -----------------------------------------------------------------------------
        // modified baseRichBolt --------------------
        private OutputCollector collector;
        // ------------------------------------------
        private Map<String, Integer> counts = new HashMap<String, Integer>();


        @Override
        @SuppressWarnings("rawtypes")
        public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
            nItems = 0;
            startTime = System.currentTimeMillis();

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
//            if (++nItems % 100000 == 0) {
//                long latency = System.currentTimeMillis() - startTime;
//                System.out.println(tuple.getString(0) + "!!!");
//                System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
//                GlobalMetrics.incr("selected_items");
//            }
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

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declarer.declare(new Fields("word"));
        }

        /**
         * Implementing this method is optional and only necessary if BOTH of the following are true:
         * <p>
         * a.) you plan to dynamically scale your bolt/spout at runtime using 'heron update'.
         * b.) you need to take action based on a runtime change to the component parallelism.
         * <p>
         * Most bolts and spouts should be written to be unaffected by changes in their parallelism,
         * but some must be aware of it. An example would be a spout that consumes a subset of queue
         * partitions, which must be algorithmically divided amongst the total number of spouts.
         * <p>
         * Note that this method is from the IUpdatable Heron interface which does not exist in Storm.
         * It is fine to implement IUpdatable along with other Storm interfaces, but implementing it
         * will bind an otherwise generic Storm implementation to Heron.
         *
         * @param heronTopologyContext Heron topology context.
         */
        @Override
        public void update(TopologyContext heronTopologyContext) {
            List<Integer> newTaskIds =
                    heronTopologyContext.getComponentTasks(heronTopologyContext.getThisComponentId());
            System.out.println("Bolt updated with new topologyContext. New taskIds: " + newTaskIds);
        }
    }
}
