package zyt.custom.scheduler.local;

import zyt.custom.utils.FileUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yitian
 * <p>
 * Attention:
 * Have abandoned.
 * This class was using for local mode.
 */
public class LatencySignalThread extends Thread {

    private static final String LOG_FILE = "/home/yitian/signal-thread-latency.txt";

    private String messageId;

    private long latency;

    private boolean isRunning = true;

    private int timeCount = 0;

    private int totalCount = 60;

    // messageid -> latency
    private Map<String, Long> latencyMap = new HashMap<>();

    // time window = 5s
    private int TIME_WINDOW_LENGTH = 5;

    public LatencySignalThread() {
    }

    public LatencySignalThread(String messageId, long latency) {
        this.messageId = messageId;
        this.latency = latency;
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
            try {
                timeCount += 1;
                if (timeCount >= totalCount) {
                    isRunning = false;
                }

                String content = this.messageId + " : " + this.latency;
                System.out.println(timeCount + " : " + content);
                FileUtils.writeToFile(LOG_FILE, timeCount + " : " + this.messageId + " : " + this.latency);

                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
