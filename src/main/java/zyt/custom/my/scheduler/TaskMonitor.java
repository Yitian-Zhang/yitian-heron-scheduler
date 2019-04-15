package zyt.custom.my.scheduler;

import com.twitter.heron.api.tuple.Tuple;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

public class TaskMonitor {

    private Logger logger = Logger.getLogger(WorkerMonitor.class);

    /**
     * This task id
     */
    private final int taskId;

    /**
     * This task -> thread id, checkThreadId to set it
     */
    private long threadId;

    /**
     * Add from yitian 2018-04-29
     * java process id
     */
    private String processId;

    private int timeWindowLength;

    /**
     * Last check time
     */
    private long lastCheck;

    /**
     * Map source task id -> number of tuples sent by source to this task
     */
    private Map<Integer, Integer> trafficStatMap;

    /**
     * Map for returning
     */
    private Map<Integer, Integer> trafficStatToReturn;

    /**
     * Constructer function
     * set task id, and init threadId = -1, it will be changed at checkThreadId function
     *
     * @param taskId
     */
    public TaskMonitor(int taskId) {
        this.taskId = taskId;
        threadId = -1;
        // add from yitian 2018-04-29
        processId = "";
        timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength() * 1000;
        trafficStatMap = new HashMap<Integer, Integer>();
    }

    /**
     * Add from yitian 2018-04-29
     * Get java process id for this thread
     * @return processId
     */
    private static String getCurrentJVMProcessId() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        System.out.println("Current jvm name is: " + runtimeMXBean.getName());
        return runtimeMXBean.getName().split("@")[0];
    }

    /**
     * Check current Thread Id whether is -1(init) or not. If it is, give the threaId currentThreadId value
     * And, registerTask to WorkerMonitor
     */
    public void checkThreadId() {
        if (threadId == -1) {
            // get current thread id
            threadId = Thread.currentThread().getId();
            // add from yitian 2018-04-29
            processId = getCurrentJVMProcessId();
            // should registerTask (this function located at WorkerMonitor.java)
            WorkerMonitor.getInstance().registerTask(this);
        }
    }

    /**
     * Storm Brench mark: WordCount bolt execute function invoke
     *
     * @param tuple current execute tuple
     */
    public void notifyTupleReceived(Tuple tuple) {
        // checkThreadId and register this taskMonitor object to WorkMonitor
        checkThreadId();

        // get number tuple sent from source to this task and record it to trafficStatMap (can get sourceTaskId in heron)
        int sourceTaskId = tuple.getSourceTask();
        Integer traffic = trafficStatMap.get(sourceTaskId);
        if (traffic == null) {
            traffic = 0;
        }
        trafficStatMap.put(sourceTaskId, ++traffic);

        // get current time and record last check time
        long now = System.currentTimeMillis();
        if (lastCheck == 0) {
            lastCheck = now;
        }
        // if the result > slotLength = timeWindowLength
        if (now - lastCheck >= timeWindowLength) {
            synchronized (this) {
                trafficStatToReturn = trafficStatMap;
                trafficStatMap = new HashMap<Integer, Integer>();
                lastCheck += timeWindowLength;
            }
        }
    }

    /*
     * source task -> number of tuples sent to this task
     * WorkerMoniter function invoke : sampleStats
     */
    public synchronized Map<Integer, Integer> getTrafficStatMap() {
        return trafficStatToReturn;
    }

    public int getTaskId() {
        return taskId;
    }

    public long getThreadId() {
        return threadId;
    }

    public String getProcessId() {
        return processId;
    }
}
