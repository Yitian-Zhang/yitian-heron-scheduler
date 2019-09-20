package zyt.custom.scheduler;

import org.apache.log4j.Logger;

/**
 * @author yitian
 */
public class MonitorConfiguration {

    private static Logger logger = Logger.getLogger(MonitorConfiguration.class);

    private static MonitorConfiguration instance = null;

    private int timeWindowCount;

    private int timeWindowLength;

    /**
     * load configuration from properties file
     */
    private MonitorConfiguration() {

        // TODO: load configuration from file
        timeWindowCount = Constants.TIME_WINDOW_COUNT;
        timeWindowLength = Constants.TIME_WINDOW_LENGTH;

        /*
        Properties properties = new Properties();
        properties.load(new FileInputStream(""));
        timeWindowCount = Integer.parseInt(properties.getProperty(""));
        timeWindowLength = Integer.parseInt(properties.getProperty(""));
        */
    }

    /**
     * Singleton pattern for creating an instance
     *
     * @return
     */
    public synchronized static MonitorConfiguration getInstance() {
        if (instance == null) {
            instance = new MonitorConfiguration();
        }
        return instance;
    }

    /**
     * return the length of monitoring time window, in seconds
     *
     * @return
     */
    public int getTimeWindowTotalLength() {
        return timeWindowLength * timeWindowCount;
    }

    public int getTimeWindowCount() {
        return timeWindowCount;
    }

    public int getTimeWindowLength() {
        return timeWindowLength;
    }
}
