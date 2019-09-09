package zyt.custom.my.scheduler.monitor;

import zyt.custom.my.scheduler.MonitorConfiguration;
import zyt.custom.my.scheduler.monitor.WorkerMonitor;

public class WorkerMonitorThread extends Thread {

    /**
     * current timeWindowLength = 10
     */
    private static int timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength();

    /**
     * Thread start in WorkerMonitor construction function.
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(timeWindowLength * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                // synchronized invoking workermoniter functions
                WorkerMonitor.getInstance().sampleStats();
                WorkerMonitor.getInstance().storeStats();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
