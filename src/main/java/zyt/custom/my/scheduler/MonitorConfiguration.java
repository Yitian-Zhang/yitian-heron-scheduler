package zyt.custom.my.scheduler;

import org.apache.log4j.Logger;

public class MonitorConfiguration {

    private static Logger logger = Logger.getLogger(MonitorConfiguration.class);

    private static MonitorConfiguration instance = null;

    private int timeWindowCount;

    private int timeWindowLength;

    /**
     * load configuration from properties file
     */
    private MonitorConfiguration() {
        logger.debug("Loading configuration from file!");
        timeWindowCount = 3;
        timeWindowLength = 5;

        // TODO: load configuration from file
        /*
        Properties properties = new Properties();
        properties.load(new FileInputStream(""));
        timeWindowCount = Integer.parseInt(properties.getProperty(""));
        timeWindowLength = Integer.parseInt(properties.getProperty(""));
        */
    }

    /**
     * Singal model for creating an instance
     *
     * @return
     */
    public synchronized static MonitorConfiguration getInstance() {
        if (instance == null)
            instance = new MonitorConfiguration();
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
