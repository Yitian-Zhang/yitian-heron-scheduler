package zyt.custom.my.scheduler;

import java.sql.SQLException;
import java.util.Map;

/**
 * ****************************************
 * Add at 2018-09-27 for monitoring the cpu usage of each host(worker node)
 *
 * this main function should be invoked by user mannel.
 * the thread will not stop if user not to stop it.
 * ****************************************
 */
public class SystemMonitorThread extends Thread {

    // 2018-09-27 add for recording the cpu usage
    private String cpuUsageFilename = "/home/yitian/logs/cpu-usage/cpu-usage.txt";
    private boolean isStart = true;

    /**
     * Cpu usage monitoring function
     * @throws SQLException
     */
    private void cpuUsageMonitor() throws SQLException {
        // 2018-09-27 add load usage of each host(worker node) -----------------------------------------
        Map<String, String> hostCpuUsageList = DataManager.getInstance().getCpuUsageOfHost();
        for (String hostname : hostCpuUsageList.keySet()) {
            System.out.println("Now:" + hostname + " : " + hostCpuUsageList.get(hostname));
            FileUtils.writeToFile(cpuUsageFilename, hostname + " : " + hostCpuUsageList.get(hostname));
        }
        // ----------------------------------------------------------------------------------
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
