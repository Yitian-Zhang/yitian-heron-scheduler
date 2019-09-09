package zyt.custom.scheduler.local;

import zyt.custom.tools.FileUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Attention:
 * Have abandoned for using. This class was using for local mode.
 */
public class LatencySignalThread extends Thread {

    private String messageId;

    private long latency;

    private boolean isRunning = true;

    private int timeCount = 0;

    private int totalCount = 60;

    private static final String LOG_FILE = "/home/yitian/signal-thread-latency.txt";

//    private String filename = "C:\\Users\\Administrator\\Desktop\\heron latency\\text.txt";
    private Map<String, Long> latencyMap = new HashMap<>(); // messageid -> latency

    private int TIME_WINDOW_LENGTH = 5; // time window = 5s

    public LatencySignalThread() {
    }

    public LatencySignalThread(String messageId, long latency) {
        this.messageId = messageId;
        this.latency = latency;
    }

    public static void main(String[] args) {

        // no synchronized test
        System.out.println("Latency Monitor started ...");
        LatencySignalThread monitor1 = new LatencySignalThread();
        LatencySignalThread monitor2 = new LatencySignalThread();
        monitor1.start(); // 启动线程
        monitor2.start();

        int i = 0;
        while (true) {
            monitor1.setContent(i+"", i);
            monitor2.setContent(i+"", i);
            i++;
        }
    }

    public void setLatencyMap(String messageId, long latency) {
        latencyMap.put(messageId, latency);
    }

    public void setContent(String messageId, long latency) {
        this.messageId = messageId;
        this.latency = latency;
    }

    @Override
    public void run() {
        while (isRunning) {
            timeCount += 1;
            if (timeCount >= totalCount) {
                isRunning = false;
            }
            String content = this.messageId + " : " + this.latency;
            System.out.println(timeCount + " : " + content);
            FileUtils.writeToFile(LOG_FILE, timeCount + " : " + this.messageId + " : " + this.latency);
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
