package zyt.custom.my.scheduler;

import java.util.Collection;
import java.util.List;

public class Utils {

    public static final int ACKER_TAKS_ID = 1;
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors"; // between 0 and 1
    public static final String ALFA = "alfa"; // between 0 and 1
    public static final String BETA = "beta"; // between 0 and 1
    public static final String GAMMA = "gamma"; // greater than 1
    public static final String DELTA = "delta"; // between 0 and 1
    public static final String TRAFFIC_IMPROVEMENT = "traffic.improvement"; // between 1 and 100
    public static final String RESCHEDULE_TIMEOUT = "reschedule.timeout"; // in s

    private Utils() {
    }

    /**
     * convert collection to string for printing
     * @param list
     * @return the list in csv format
     */
    public static String collectionToString(Collection<?> list) {
        if (list == null) {
            return "null";
        }
        if (list.isEmpty()) {
            return "<empty list>";
        }
        StringBuffer stringBuffer = new StringBuffer();
        int i = 0;
        for (Object item : list) {
            stringBuffer.append(item.toString());
            if (i < list.size() - 1) {
                stringBuffer.append(", ");
            }
            i++;
        }
        return stringBuffer.toString();
    }

    public static Executor getExecutor(int taskId, List<Executor> executorList) {
        for (Executor executor : executorList) {
            if (executor.getBeginTask() == taskId) {
                return executor;
            }
        }
        return null;
    }
}
