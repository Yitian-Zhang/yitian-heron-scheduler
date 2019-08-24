package zyt.custom.my.scheduler;

public class LatencyMonitorThread extends Thread {

    /**
     * Time window length
     */
    private static int timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength();

//    private String filename = "/home/yitian/logs/latency-monitor.txt";

    @Override
    public void run() {
        while (true) {
            // simple for output - 20180921
//            FileUtils.writeToFile(filename, "Latency Monitor running... ");
            LatencyMonitor.getInstance().storeLatency();
            try {
                Thread.sleep(timeWindowLength * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
