package zyt.custom.scheduler.monitor;

import com.twitter.heron.api.topology.TopologyContext;
import org.apache.log4j.Logger;
import zyt.custom.cpuinfo.CPUInfo;
import zyt.custom.scheduler.Constants;
import zyt.custom.scheduler.DataManager;
import zyt.custom.scheduler.MonitorConfiguration;
import zyt.custom.scheduler.component.Executor;
import zyt.custom.scheduler.component.TaskPair;
import zyt.custom.tools.FileUtils;
import zyt.custom.tools.Utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yitian
 */
public class WorkerMonitor {
    private Logger logger = Logger.getLogger(WorkerMonitor.class);

    private static WorkerMonitor instance = null;

    /**
     * Worker corresponding topology
     */
    private String topologyId;

    /**
     * unused
     */
    private int workerPort;

    /**
     * ThreadId -> time series of the load
     */
    private Map<Long, List<Long>> loadStats;

    /**
     * <sourceTaskId, destinationTaskId> -> time series of the traffic
     */
    private Map<TaskPair, List<Integer>> trafficStats;

    /**
     * threadId -> list of tasks id, in the formed [begin task, end task] = Executor
     * A thread can running some task in storm.
     * It has changed in heron. There is only one task in a thread.
     */
    private Map<Long, Executor> threadToTaskMap;

    private List<TaskMonitor> taskMonitorList;

    private int timeWindowCount;

    private int timeWindowLength;

    /**
     * 2018-09-27 add for recording the cpu usage
     */
//    private String cpuUsageFilename = "/home/yitian/logs/cpu-usage.txt";


    private WorkerMonitor() {
        loadStats = new HashMap<>();
        trafficStats = new HashMap<>();
        threadToTaskMap = new HashMap<>();
        taskMonitorList = new ArrayList<>();

        timeWindowCount = MonitorConfiguration.getInstance().getTimeWindowCount();
        timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength();

        // should invoke DataManager to check node state. 2018-05-08
        try {
            DataManager.getInstance().checkNode(CPUInfo.getInstance().getTotalSpeed());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // record cpu total speed
        logger.info("[WORKER-MONITOR] - Cpu total speed: " + CPUInfo.getInstance().getTotalSpeed());

        // start worker monitor thread to invoke function in this class
        new WorkerMonitorThread().start();
        logger.info("[WORKER-MONITOR] - WorkerMonitor started!");
    }

    public synchronized static WorkerMonitor getInstance() {
        if (instance == null) {
            instance = new WorkerMonitor();
        }
        return instance;
    }

    /**
     * invoke in TaskMonitor notifyTupleReceived -> checkThreadId function
     *
     * @param taskMonitor
     */
    public synchronized void registerTask(TaskMonitor taskMonitor) {
        logger.info("[WORKER-MONITOR] - RegisterTask to taskMonitorList, current task id: " + taskMonitor.getTaskId());

        Executor executor = threadToTaskMap.get(taskMonitor.getThreadId());
        if (executor == null) {
            executor = new Executor();
            // add taskMonitor <threadID, <id -> taskId list> to threadToTaskMap
            threadToTaskMap.put(taskMonitor.getThreadId(), executor);
        }

        // add new task id in a thread [beginTask -> endTask]
        if (!executor.includes(taskMonitor.getTaskId())) {
            executor.add(taskMonitor.getTaskId());
        }

        // add to taskMonitorList
        taskMonitorList.add(taskMonitor);

        // 2018-07-23 annotated for simple logs
        /*
        logger.info("----------Output threadToTaskMap----------");
        for (long threadId : threadToTaskMap.keySet()) {
            logger.debug("- " + threadId + ": " + threadToTaskMap.get(threadId));
        }
        logger.info("----------Output taskMoniterList----------");
        for (TaskMonitor monitor : taskMonitorList) {
            logger.debug("ThreadId: " + monitor.getThreadId() + " TaskId: " + monitor.getTaskId());
        }
        logger.info("------------------------------------------");
        */
    }

    /**
     * invoke at WorkerMonitorThread
     */
    public synchronized void sampleStats() {
        logger.info("[WORKER-MONITOR] - Starting sampleStats...");

        // record traffic, there are each tackMonitor in each spout or bolt
        for (TaskMonitor taskMonitor : taskMonitorList) {
            // trafficStatMap: source task id -> number of tuples sent by source to this task
            Map<Integer, Integer> taskTrafficStats = taskMonitor.getTrafficStatMap();
            if (taskTrafficStats != null) {
                for (int sourceTaskId : taskTrafficStats.keySet()) {
                    // invoke notifyLoadStat function in this class
                    notifyTrafficStat(new TaskPair(sourceTaskId, taskMonitor.getTaskId()),
                            taskTrafficStats.get(sourceTaskId));
                }
            }
        }

        // load : invoke loadMonitor function
        Map<Long, Long> loadInfo = LoadMonitor.getInstance().getLoadInfo(threadToTaskMap.keySet());
        for (long threadId : loadInfo.keySet()) {
            notifyLoadStat(threadId, loadInfo.get(threadId));
        }
    }

    /**
     * @param taskPair <sourceTaskId, destinationTaskId>
     * @param traffic  sourceTaskId -> destinationTaskId: tuple traffic
     */
    private void notifyTrafficStat(TaskPair taskPair, int traffic) {
        logger.info("[WORKER-MONITOR] - Notify traffic stats...");

        // trafficStats: <sourceTaskId, destinationTaskId> -> time series of the traffic
        // get traffic of this taskPair from trafficStats
        List<Integer> trafficList = trafficStats.get(taskPair);
        if (trafficList == null) { // if is initiable
            trafficList = new ArrayList<Integer>();
            trafficStats.put(taskPair, trafficList);
        }
        trafficList.add(traffic);

        // keep traffic record number of one TaskPair is timeWindowCount
        if (trafficList.size() > timeWindowCount) {
            trafficList.remove(0);
        }
    }

    /**
     * Notify load stat
     *
     * @param threadId thread id
     * @param load     thread load (cpu time * cpu speed)
     */
    private void notifyLoadStat(long threadId, long load) {
        logger.info("[WORKER-MONITOR] - Start notify load stat...");

        // loadStats: ThreadId -> time series of the load
        List<Long> loadList = loadStats.get(threadId);
        if (loadList == null) {
            loadList = new ArrayList<Long>();
            loadStats.put(threadId, loadList);
        }
        loadList.add(load);
        if (loadList.size() > timeWindowCount) {
            loadList.remove(0);
        }
    }

    /**
     * Store stats to logger files (not to store DB)
     */
    public void storeStats() throws Exception {
        logger.info("[WORKER-MONITOR] - Start store stats into file. TopologyId: " + topologyId);

        logger.debug("------------------WORKER MONITOR SNAPSHOT START------------------");
        logger.debug("Current InterNode Traffic is: " + DataManager.getInstance().getCurrentInterNodeTraffic());
        logger.debug("This Topology current total load is: " + DataManager.getInstance().getTotalLoad(topologyId));
        // 2018-07-23 add for simple logs
        /*
        if (taskMonitorList.size() != 0) {
            FileUtils.writeToFile(trafficFilename, taskMonitorList.get(0).getTaskId() + " : " + DataManager.getInstance().getCurrentInterNodeTraffic());
        }
        */
        FileUtils.writeToFile(Constants.TRAFFIC_DATA_FILE, taskMonitorList.get(0).getTaskId() + " : " + DataManager.getInstance().getCurrentInterNodeTraffic());

        // Output the threadToTaskMap
        logger.debug("Threads to Tasks association is: ");
        for (long threadId : threadToTaskMap.keySet()) {
            logger.debug(" - " + threadId + ": " + threadToTaskMap.get(threadId));
        }

        logger.debug("Inter-Task Traffic Stats (tuples sent per time slot): ");
        for (TaskPair pair : trafficStats.keySet()) {
            logger.debug(" - " + pair.getSourceTaskId() + " -> " + pair.getDestinationTaskId() + ": " + getTraffic(pair) + " tuple/s [" + Utils.collectionToString(trafficStats.get(pair)) + "]");
            // invoke DataManager function to store Traffic info. 2018-05-08
            DataManager.getInstance().storeTraffic(topologyId, pair.getSourceTaskId(), pair.getDestinationTaskId(), getTraffic(pair));
        }

        logger.debug("Load stats (CPU cycles consumed per time slot): ");
        long totalCPUCyclesPerSecond = 0;
        for (long threadId : loadStats.keySet()) {
            List<Long> threadLoadInfo = loadStats.get(threadId);
            totalCPUCyclesPerSecond += threadLoadInfo.get(threadLoadInfo.size() - 1) / timeWindowLength;
            logger.debug(" - thread " + threadId + ": " + getLoad(threadId) + " cycle/s [" + Utils.collectionToString(threadLoadInfo) + "]");

            Executor executor = threadToTaskMap.get(threadId);
            // invoke DataMananger to store load info. 2018-05-08
            DataManager.getInstance().storeCpuLoad(topologyId, executor.getBeginTask(), executor.getEndTask(), getLoad(threadId));

        }
        long totalCPUCyclesAvailable = CPUInfo.getInstance().getTotalSpeed();
        double usage = ((double) totalCPUCyclesPerSecond / totalCPUCyclesAvailable) * 100; // int -> double
        logger.debug("Total CPU cycles consumed per second: " + totalCPUCyclesPerSecond + ", Total available: " + totalCPUCyclesAvailable + ", Usage: " + usage + "%");

        // add from yitian 2018-04-29
        logger.debug("Output the TaskMonitorList: ");
        for (TaskMonitor monitor : taskMonitorList) {
            logger.debug("- ProcessId: " + monitor.getProcessId() + " -> threadId: " + monitor.getThreadId() + " -> taskId: " + monitor.getTaskId());
        }

        // 2018-09-27 add load usage of each host(worker node)
        /*
        Map<String, String> hostCpuUsageList = DataManager.getInstance().getCpuUsageOfHost();
        for (String hostname : hostCpuUsageList.keySet()) {
            FileUtils.writeToFile(cpuUsageFilename, hostname + " : " + hostCpuUsageList.get(hostname));
        }
        */
        logger.debug("------------------WORKER MONITOR SNAPSHOT END------------------");
    }

    /**
     * invoke at storeStats function
     * computing the total average traffic in a timeWindowLength and traffic data record count
     *
     * @param pair
     * @return average tuples per second sent by pair.source to pair.destination
     */
    private int getTraffic(TaskPair pair) {
        int total = 0;
        List<Integer> trafficData = trafficStats.get(pair);
        for (int traffic : trafficData) {
            total += traffic;
        }
        return (int) ((float) total / (trafficData.size() * timeWindowLength)); // tuples/s
    }

    /**
     * invoke at storeStats function
     *
     * @param threadId
     * @return average CPU cycles per second consumed by threadID
     */
    private long getLoad(long threadId) {
        long total = 0;
        List<Long> loadData = loadStats.get(threadId);
        for (long load : loadData) {
            total += load;
        }
        return total / (loadData.size() * timeWindowLength);
    }

    /**
     * Set context info of the topology
     *
     * @param context topologyId
     */
    public void setContextInfo(TopologyContext context) {
        this.topologyId = context.getTopologyId();
        logger.info("[WORKER-MONITOR] - This ComponentId is: " + context.getThisComponentId());

        // TODO: multi-topologies
        /*
            invoke DataManager: checkTopology to check topology
            DataManager.getInstance().checkTopology(topologyId);
         */
    }

    public String getTopologyId() {
        return topologyId;
    }
}
