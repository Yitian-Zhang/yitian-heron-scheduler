package zyt.custom.scheduler.monitor;

import zyt.custom.scheduler.MonitorConfiguration;

public class LatencyMonitorThread extends Thread {

    /**
     * Time window length
     */
    private static int timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength();

    @Override
    public void run() {
        while (true) {
            LatencyMonitor.getInstance().storeLatency();
            try {
                Thread.sleep(timeWindowLength * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
