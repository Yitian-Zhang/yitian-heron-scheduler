package zyt.custom.my.scheduler;

public class LatencyMonitorThread extends Thread {
//    private boolean isRunning = true;
//    private int timeCount = 0;
//    private int totalCount = 60;
//    private static int timeWindowLength = 5; // MonitorConfiguration is 10s
    private static int timeWindowLength = MonitorConfiguration.getInstance().getTimeWindowLength(); // 10

//    private String filename = "/home/yitian/logs/latency-monitor.txt";

    @Override
    public void run() {
        while (true) {
            // simple for output - 20180921
//            FileUtils.writeToFile(filename, "Latency Monitor running... ");
            LatencyMonitor.getInstance().storeLatency();
            try {
                Thread.sleep( timeWindowLength * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
