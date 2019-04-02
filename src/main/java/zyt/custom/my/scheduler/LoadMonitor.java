package zyt.custom.my.scheduler;

import zyt.custom.cpuinfo.CPUInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LoadMonitor {

    private static final int SECS_TO_NANOSECS = 1000000000; // transfer Seconds to nano seconds
    private static LoadMonitor instance = null;
    private final long cpuSpeed; // Hz
    Map<Long, Long> loadHistory;

    public static LoadMonitor getInstance() {
        if (instance == null)
            instance = new LoadMonitor();
        return instance;
    }

    private LoadMonitor() {
        // get core=0 processor's speed to cpuSpeed. Why core=0 ?????
        cpuSpeed = CPUInfo.getInstance().getCoreInfo(0).getSpeed();
    }

    public Map<Long, Long> getLoadInfo(Set<Long> threadIds) {
        // get current load
        Map<Long, Long> currentLoadInfo = new HashMap<Long, Long>();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (long id : threadIds)
            currentLoadInfo.put(id, threadMXBean.getThreadCpuTime(id)); // get current thread load (cpu time)
        // get cpu load
        Map<Long, Long> loadInfo = new HashMap<Long, Long>();
        for (long id : threadIds) {
            long old = 0;
            if (loadHistory != null && loadHistory.get(id) != null)
                old = loadHistory.get(id);
            double deltaTime = (double) (currentLoadInfo.get(id) - old) / SECS_TO_NANOSECS;
            loadInfo.put(id, (long) (deltaTime * cpuSpeed));
        }
        loadHistory = currentLoadInfo; // use current load to
        return loadInfo; // return current load info
    }

}
