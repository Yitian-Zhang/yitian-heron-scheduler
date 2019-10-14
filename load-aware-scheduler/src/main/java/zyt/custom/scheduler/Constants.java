package zyt.custom.scheduler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Monitor Constants Variables
 *
 * @author yitian
 */
public class Constants {

    /**
     * transfer Seconds to nano seconds
     */
    public static final int SECS_TO_NANOSECS = 1000000000;

    /**
     * latency-data file URL
     */
    public static final String LATENCY_FILE = "/home/yitian/logs/latency/aurora/latency-data.txt";

    /**
     * For recording the cpu usage
     */
    public static final String CPU_USAGE_FILE = "/home/yitian/logs/cpu-usage/cpu-usage.txt";

    /**
     * For recoding traffic data
     */
    public static final String TRAFFIC_DATA_FILE = "/home/yitian/logs/traffic-data.txt";

    /**
     * date format pattern
     */
    public static DateFormat DATE_FORMAT_PATTERN = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * config file for DB in each host
     */
    public static final String DATABASE_CONFIG_FILE = "/home/yitian/heron-conf/db.ini";

    /**
     * config file for DBCP
     */
    public static final String DBCP_CONFIG_FILE = "dbcp.properties";

    /**
     * time window count in MonitorConfiguration
     */
    public static final Integer TIME_WINDOW_COUNT = 3;

    /**
     * time window length in MonitorConfiguration
     */
    public static final Integer TIME_WINDOW_LENGTH = 10;

    /**
     * AuroraSchedulerController log file
     */
    public static final String SCHEDULER_LOG_FILE = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";

    // original constants
    /*
    public static final int ACKER_TAKS_ID = 1;
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors"; // between 0 and 1
    public static final String ALFA = "alfa"; // between 0 and 1
    public static final String BETA = "beta"; // between 0 and 1
    public static final String GAMMA = "gamma"; // greater than 1
    public static final String DELTA = "delta"; // between 0 and 1
    public static final String TRAFFIC_IMPROVEMENT = "traffic.improvement"; // between 1 and 100
    public static final String RESCHEDULE_TIMEOUT = "reschedule.timeout"; // in s
     */
}
