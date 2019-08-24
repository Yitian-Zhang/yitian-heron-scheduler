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

package zyt.custom.topology.aurora.spout;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.SysUtils;
import zyt.custom.my.scheduler.LatencyMonitor;
import zyt.custom.my.scheduler.TaskMonitor;
import zyt.custom.my.scheduler.WorkerMonitor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TestWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -3217886193225455451L;
    private final Duration throttleDuration;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;

    // deployed load monitor -------------------------------------------------------
    private TaskMonitor taskMonitor;
    // -----------------------------------------------------------------------------
    // deployed latency monitor ------------------------
    private long spoutStartTime;
    private int taskId;
    private Map<String,Long> startTimeMap;
    private static long filedCount = 0;
    private static final String FILED_COUNT_FILE = "/home/yitian/logs/failed-count.txt";
    // -------------------------------------------------

    public TestWordSpout() {
        this(Duration.ZERO);
    }

    public TestWordSpout(Duration throttleDuration) {
        this.throttleDuration = throttleDuration;
    }

    @SuppressWarnings("rawtypes")
    public void open(
            Map conf,
            TopologyContext topologyContext,
            SpoutOutputCollector acollector) {

        // deployed load monitor -------------------------------------------------------
        WorkerMonitor.getInstance().setContextInfo(topologyContext); // start thread and set topology (check topology)
        taskMonitor = new TaskMonitor(topologyContext.getThisTaskId());
        // -----------------------------------------------------------------------------

        collector = acollector;
        words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
        rand = new Random();

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


    public void close() {
    }

    public void nextTuple() {
        // deployed load monitor -------------------------------------------------------
        taskMonitor.checkThreadId();
        // -----------------------------------------------------------------------------
        // deployed latency monitor ------------------------
        spoutStartTime = System.currentTimeMillis(); // record spout start time
        String uuid = generateUUID();
        startTimeMap.put(uuid, spoutStartTime); // msgid->starttime
        // -----------------------------------------------------------

        final String word = words[rand.nextInt(words.length)];
        collector.emit(new Values(word, uuid));

        if (!throttleDuration.isZero()) {
            SysUtils.sleep(throttleDuration); // sleep to throttle back cpu usage
        }
    }

    public void ack(Object msgId) {
        // deployed latency monitor ------------------------
        String messageId = (String) msgId; // message id
        long startTime = startTimeMap.get(messageId);
        startTimeMap.remove(messageId);
        // compute spout latency
//            long latency = System.currentTimeMillis() - spoutStartTime;
        long latency = System.currentTimeMillis() - startTime;
        LatencyMonitor.getInstance().setContent(String.valueOf(taskId), latency);
        // -------------------------------------------------
    }

    public void fail(Object msgId) {
        // 20181023 add for record the filed tuple number
//            filedCount += 1;
//            FileUtils.writeToFile(FILED_COUNT_FILE, ""+ filedCount);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
