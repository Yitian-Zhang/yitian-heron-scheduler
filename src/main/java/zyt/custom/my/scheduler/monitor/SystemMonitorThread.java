package zyt.custom.my.scheduler.monitor;

import zyt.custom.my.scheduler.DataManager;
import zyt.custom.tools.FileUtils;

import java.sql.SQLException;
import java.util.Map;

/**
 * Add: 2018-09-27
 * For monitoring the cpu usage of each host (worker node)
 * <p>
 * This main function should be invoked by user manually.
 * The thread will not stop if user not to stop it.
 */
public class SystemMonitorThread extends Thread {

    /**
     * Add: 2018-09-27
     * For recording the cpu usage
     */
    private static final String CPU_USAGE_FILE = "/home/yitian/logs/cpu-usage/cpu-usage.txt";

    private boolean isStart = true;

    // main function
    public static void main(String[] args) {
        new SystemMonitorThread().start();
    }

    /**
     * Cpu usage monitoring function
     *
     * @throws SQLException
     */
    private void cpuUsageMonitor() throws SQLException {
        // 2018-09-27 add load usage of each host(worker node)
        Map<String, String> hostCpuUsageList = DataManager.getInstance().getCpuUsageOfHost();
        for (String hostname : hostCpuUsageList.keySet()) {
            System.out.println("Now:" + hostname + " : " + hostCpuUsageList.get(hostname));
            FileUtils.writeToFile(CPU_USAGE_FILE, hostname + " : " + hostCpuUsageList.get(hostname));
        }
    }

    @Override
    public void run() {
        while (isStart) {
            try {
                Thread.sleep(10 * 1000);
                cpuUsageMonitor();
            } catch (InterruptedException | SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
