package zyt.custom.scheduler.monitor;

import org.apache.commons.collections4.CollectionUtils;
import zyt.custom.scheduler.Constants;
import zyt.custom.scheduler.MonitorConfiguration;
import zyt.custom.utils.FileUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yitian
 */
public class LatencyMonitor {

    private static LatencyMonitor instance = null;

    /**
     * latency monitor file URL
     * Windows: filename = "D:\\logs\\latecny-test.txt";
     */
    private static final String LOG_FILE = "/home/yitian/logs/latency/aurora/latency-monitor.txt";

    /**
     * taskId -> a list of latency in time window (5s)
     */
    private Map<String, List<Long>> latencyMap;

    private Map<String, List<Long>> latencyMapReturn;

    private long lastCheck;

    private int timeWindowLength;

    private int timeWindowCount;

    public LatencyMonitor() {
        FileUtils.writeToFile(LOG_FILE, "Construction LatencyMonitor.");
        timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength() * 1000;
        latencyMap = new HashMap<>();
        latencyMapReturn = new HashMap<>();
        timeWindowCount = 0;
        lastCheck = 0;

        // start latency monitor thread
        new LatencyMonitorThread().start();
        FileUtils.writeToFile(LOG_FILE, "Latency Monitor Thread started.");
    }

    public synchronized static LatencyMonitor getInstance() {
        if (instance == null) {
            instance = new LatencyMonitor();
        }
        return instance;
    }

    /**
     * Construct the latencyMap for every task
     *
     * @param taskId
     * @param latency
     */
    public void setContent(String taskId, long latency) {
        // get the latency list of the taskId
        List<Long> latencyList = latencyMap.get(taskId);
        if (CollectionUtils.isEmpty(latencyList)) {
            latencyList = new ArrayList<>();
            latencyMap.put(taskId, latencyList);
        }
        latencyList.add(latency);

        long now = System.currentTimeMillis();
        if (lastCheck == 0) {
            lastCheck = now;
        }
        if (now - lastCheck >= timeWindowLength) {
            synchronized (this) {
                latencyMapReturn = latencyMap;
                latencyMap = new HashMap<>();
                lastCheck += timeWindowLength;
                FileUtils.writeToFile(LOG_FILE, "Synchronized Latency Map.");
            }
        }
    }

    /**
     * Output latency data into a file
     */
    public synchronized void storeLatency() {
        FileUtils.writeToFile(LOG_FILE, "-----------------Starting Store Latency----------------");
        timeWindowCount += 1;

        FileUtils.writeToFile(LOG_FILE, "Get this latency return map...");
        Map<String, List<Long>> latencyMap = this.getLatencyMap();
        if (latencyMap == null) {
            FileUtils.writeToFile(LOG_FILE, "Current Latency Map is empty!!!");
        }

        // null pointer exception, new HashMap in construction function to solve this problem
        for (String taskId : latencyMap.keySet()) {
            FileUtils.writeToFile(LOG_FILE, "Current store taskId: " + taskId);
            // get the latency list of the current taskid
            List<Long> latencyList = latencyMap.get(taskId);

            // get the size of the latency list
            int count = latencyList.size();
            FileUtils.writeToFile(LOG_FILE, "This tuple latency list size: " + count);

            double totalLatency = 0.0;
            for (Long latency : latencyList) {
                totalLatency += latency;
            }

            FileUtils.writeToFile(LOG_FILE, "The total tuple latency in time window: " + totalLatency);
            double averageLatency = totalLatency / count;
            FileUtils.writeToFile(Constants.LATENCY_FILE, taskId + " : " +
                    String.format("%.2f", averageLatency) + " -> in windows: " + timeWindowCount);
        }
        FileUtils.writeToFile(LOG_FILE, "-------------------End Store Latency------------------");
    }

    public synchronized Map<String, List<Long>> getLatencyMap() {
        return latencyMapReturn;
    }

}
