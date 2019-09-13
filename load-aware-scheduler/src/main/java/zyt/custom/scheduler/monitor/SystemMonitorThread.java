package zyt.custom.scheduler.monitor;

import zyt.custom.scheduler.Constants;
import zyt.custom.scheduler.DataManager;
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

    private boolean isStart = true;

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
            FileUtils.writeToFile(Constants.CPU_USAGE_FILE, hostname + " : " + hostCpuUsageList.get(hostname));
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

    // main function
    public static void main(String[] args) {
        new SystemMonitorThread().start();
    }
}
