package zyt.custom.my.scheduler;

import com.twitter.heron.api.tuple.Tuple;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

public class TaskMonitor {
    private Logger logger = Logger.getLogger(WorkerMonitor.class);
    private final int taskId; // this task id
    private long threadId; // this task -> thread id, checkThreadId to set it
    // add from yitian 2018-04-29 ------------------------------
    private String processId;
    // ---------------------------------------------------------
    private int timeWindowLength; // slotLength -> timeLength: ms.
    private long lastCheck; // last check time
    // map source task id -> number of tuples sent by source to this task
    private Map<Integer, Integer> trafficStatMap;
    // return map
    private Map<Integer, Integer> trafficStatToReturn;

    /**
     * Constructer function
     * set task id, and init threadId = -1, it will be changed at checkThreadId function
     * @param taskId
     */
    public TaskMonitor(int taskId) {
        this.taskId = taskId;
        threadId = -1;
        // add from yitian 2018-04-29 ------------------------------
        processId = "";
        // ---------------------------------------------------------
//        timeLength = 3 * 1000; // 3 s
        timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength() * 1000; // 10s
        trafficStatMap = new HashMap<Integer, Integer>();
    }

    /**
     * Check current Thread Id whether is -1(init) or not. If it is, give the threaId currentThreadId value
     * And, registerTask to WorkerMonitor
     */
    public void checkThreadId() {
        if (threadId == -1) {
            threadId = Thread.currentThread().getId(); // get current thread id
            // add from yitian 2018-04-29 ------------------------------
            processId = getCurrentJVMProcessId();
            // ---------------------------------------------------------
            // should registerTask (this function located at WorkerMonitor.java)
            WorkerMonitor.getInstance().registerTask(this);
        }
    }

    // add from yitian 2018-04-29 ------------------------------
    private static String getCurrentJVMProcessId() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        System.out.println("Current jvm name is: " + runtimeMXBean.getName());
        return runtimeMXBean.getName().split("@")[0];
    }
    // ---------------------------------------------------------

    /**
     * Storm Brench mark: WordCount bolt execute function invoke
     * @param tuple current execute tuple
     */
    public void notifyTupleReceived(Tuple tuple) {
//        logger.info("Invoking nofifyTupleReceived function...");
        checkThreadId(); // checkThreadId and register this taskMonitor object to WorkMonitor
        int sourceTaskId = tuple.getSourceTask(); // can get this value in heron
        Integer traffic = trafficStatMap.get(sourceTaskId); // get number tuple sent from source to this task
        if (traffic == null)
            traffic = 0;
        trafficStatMap.put(sourceTaskId, ++traffic); // record sourceTask traffic

        long now = System.currentTimeMillis(); // get current time
        if (lastCheck == 0)
            lastCheck = now; // record last check time
        if (now - lastCheck >= timeWindowLength) { // if the result > slotLength =  timeWindowLength
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

    public String getProcessId() { return processId; }
}
