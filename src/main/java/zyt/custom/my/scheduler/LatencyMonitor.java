package zyt.custom.my.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LatencyMonitor {

    private String messageId; // unuse
    private String taskId; // unuse
//    private String filename = "D:\\logs\\latecny-test.txt";
    private String filename = "/home/yitian/logs/latency/aurora/latency-monitor.txt";
    // 2018-07-23 add for simple logs -------------------------
    private String latencyFilename = "/home/yitian/logs/latency/aurora/latency-data.txt";
    // --------------------------------------------------------
    private static LatencyMonitor instance = null;
    // taskId -> a list of latency in 5s
    private Map<String, List<Long>> latencyMap;
    private Map<String, List<Long>> latencyMapReturn;
    private long lastCheck;
    private int timeWindowLength;
    private int timeWindowCount;

    public synchronized static LatencyMonitor getInstance() {
        if (instance == null)
            instance = new LatencyMonitor();
        return instance;
    }

    /**
     * construction function without args
     */
    public LatencyMonitor() {
        FileUtils.writeToFile(filename, "Construction LatencyMonitor...");
        timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength() * 1000;
//        timeWindowLength = 5 * 1000; // time window lentgh = 5s
        latencyMap = new HashMap<>();
        latencyMapReturn = new HashMap<>();
        timeWindowCount = 0;
        lastCheck = 0;
        new LatencyMonitorThread().start(); // 启动线程
        FileUtils.writeToFile(filename, "Latency Monitor Thread started...");
    }

    /**
     * 在5秒内加入tuple，并统计
     * @param taskId
     * @param latency
     */
    public void setContent(String taskId, long latency) {
//        FileUtils.writeToFile(filename, "Task id: " + taskId + " and tuple latency: " + latency);
        List<Long> latencyList = latencyMap.get(taskId); // 获取当前taskId对应的latency列表
        if (latencyList == null) {
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
                FileUtils.writeToFile(filename, "Synchronized Latency Map...");
                latencyMapReturn = latencyMap;
                latencyMap = new HashMap<>();
                lastCheck += timeWindowLength;
            }
        }
    }

    /**
     * 输出统计结果到文件
     */
    public synchronized void storeLatency() {
        FileUtils.writeToFile(filename, "-----------------Starting Store Latency----------------");
        timeWindowCount += 1;
        FileUtils.writeToFile(filename, "Get this latency return map...");
        Map<String, List<Long>> latencyMap = this.getLatencyMap();
        if (latencyMap == null) { //
            FileUtils.writeToFile(filename, "Current Latency Map is empty!!!");
        }
        for (String taskId : latencyMap.keySet()) { // null pointer exception, new HashMap in construction function to solve this problem
            FileUtils.writeToFile(filename, "Current store taskId: " + taskId);
            List<Long> latencyList = latencyMap.get(taskId); // 获取当前taskId的tupleLatency List
            int count = latencyList.size(); // timeWindowLength内记录到的tuple数量
            FileUtils.writeToFile(filename, "This tuple latency list size: " + count);
            double totalLatency = 0.0;
            for (Long latency : latencyList) {
                totalLatency += latency;
            }
            FileUtils.writeToFile(filename, "The total tuple latency in time window: " + totalLatency);
            double averageLatency = totalLatency / count;
            FileUtils.writeToFile(this.filename, "Current task id: " + taskId + " and the average latency: " + averageLatency + " in window: " + timeWindowCount);
            // 2018-07-23 add for simple logs -------------------------
            FileUtils.writeToFile(latencyFilename, taskId + " : " + String.format("%.2f", averageLatency) + " -> in windows: " + timeWindowCount);
            // --------------------------------------------------------
        }
        FileUtils.writeToFile(filename, "-------------------End Store Latency------------------");
    }

    public synchronized Map<String, List<Long>> getLatencyMap() {
        FileUtils.writeToFile(filename, "Invoke Latency Return Map Function...");
        return latencyMapReturn;
    }

    public static void main(String[] args) {
        List<String> taskIdList = new ArrayList<>();
        taskIdList.add("001");
        taskIdList.add("002");
        for (int i = 0; i < 10000; i++) {
            for (String taskId : taskIdList) {
                LatencyMonitor.getInstance().setContent(taskId, i);
            }
        }
    }
}
