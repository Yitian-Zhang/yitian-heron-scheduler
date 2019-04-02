package zyt.custom.my.scheduler;

import org.apache.log4j.Logger;

public class MonitorConfiguration {

    private static MonitorConfiguration instance = null;
    private int timeWindowCount;
    private int timeWindowLength;
    private Logger logger;

    /**
     * load configuration from properties file
     */
    private MonitorConfiguration() {
        logger = Logger.getLogger(MonitorConfiguration.class);

        // load configuration from file
        logger.debug("Loading configuration from file!");
        // ----
//        Properties properties = new Properties();
//            properties.load(new FileInputStream(""));
//            timeWindowCount = Integer.parseInt(properties.getProperty(""));
//            timeWindowLength = Integer.parseInt(properties.getProperty(""));
        timeWindowCount = 3; // ?????
        timeWindowLength = 5; //
    }

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
