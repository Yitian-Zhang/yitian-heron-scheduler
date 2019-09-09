package zyt.custom.scheduler.aurora;

import com.twitter.heron.spi.packing.PackingPlan;
import zyt.custom.scheduler.DataManager;
import zyt.custom.tools.FileUtils;
import zyt.custom.scheduler.component.Node;

import java.sql.SQLException;
import java.util.List;

/**
 * AuroraCustomScheduler
 * ADD: 2018-07-04
 *
 * AuroraSchedulerController
 * - TriggerSchedule
 */
public class AuroraSchedulerThread extends Thread {

    /**
     * Log file
     */
    private static final String FILE_NAME = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";

    /**
     * 20181117 for trigger rescheduling of Load-aware scheduling
     */
    private static final int RESCHEDULING_THRESHOLD = 20;

    private long lastRescheduling;

    /**
     * while loop threshold value (60s)
     */
    private int count = 0;

    private PackingPlan packingPlan;

    private AuroraSchedulerController schedulerController;

    private String topologyName;

    private boolean isStart = true;

    public AuroraSchedulerThread() {
    }

    public AuroraSchedulerThread(String topologyName, PackingPlan packingPlan, AuroraSchedulerController schedulerController) {
        this.topologyName = topologyName;
        this.packingPlan = packingPlan;
        this.schedulerController = schedulerController;
    }


    /**
     * Original trigger conditions backup in 20181117
     * For DSC-Heron rescheduling algorithm
     */
    /*
    @Override
    public void run() {
        while (isStart) {
            try {
                Thread.sleep(10 * 1000);
                FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Starting [NO. " + count + 1 + " times] WorkNode load searching...");
                // modified at: 0722, from 18->28
                // modified at: 0726, 28-> 34
                // modified at: 0728, 34->39
                if (count >= 30) { // 18 * 10s / default: 28*10s for dsc-heron
                    // invoke trigger function
                    FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Starting this time Rescheduling...");
                    FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Have past 180s, last schedule time is: " + lastRescheduling + ". Triggerring scheduling...");
                    // *******************CUSTOM SCHEDULER INVOKED*********************
                    // invoked trigerSchedule function in AuroraSchedulerController class
//                    schedulerController.triggerSchedule(packingPlan);
                    // invoking DSC-Heron
                    schedulerController.hotEdgeSchedule(packingPlan);
                    // invoking BW-Heron
//                    schedulerController.basedWeightSchedule(packingPlan);
                    // invoke test-schedule
//                    schedulerController.testSchedule(packingPlan);
                    // ****************************************************************
                    // 2018-09-21 deleted for bw algorithm
//                    if (!DataManager.getInstance().calculateHotEdges(topologyName).isEmpty()) {
////                                hotEdgeSchedule(packingPlan);
//                    } else {
//                        FileUtils.writeToFile(filename, "Now, there is no hot-edge, Waiting another 180s...");
//                    }
                    count = 0;
                    lastRescheduling = System.currentTimeMillis();
                    isStart = false; // 0714 add: stop thread after update topology one time
                }
//                List<Node> nodeList = DataManager.getInstance().getOverloadedNodes();
//                if (nodeList.size() != 0) { // exist over loaded node in cluster
//                    for (Node node : nodeList) {
//                        FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] -Overloaded node: " + node + " has overloaded :" + count + 1 + " times!!!");
//                    }
//                } else {
//                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] -No node is overloaded...");
//                }
                count++;
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    */

    /**
     * 20181117 new trigger conditions build
     * for load-aware online scheduling algorithm
     */
    @Override
    public void run() {
        // firstly sleep 240s for starting the cluster
        try {
            System.out.println("[SCHEDULE-THREAD] - Started AuroraSchedulerThread. First sleep for 240s ... please waiting...");
            Thread.sleep(240 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // while loop
        while (isStart) {
            try {
                System.out.println("[SCHEDULE-THREAD] - Then sleep for 10s ... Starting WorkNode load searching...");
                Thread.sleep(10 * 1000);
//                FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Starting WorkNode load searching...");

                // if continue to 60s, the different percentage > 20% (reschedling threshold), trigger the rescheduling
                if (count >= 6) {
                    // invoke trigger function
                    FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Starting Rescheduling...");
                    FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Last schedule time is: " + lastRescheduling + ".");

                    // Triggered the rescheduling...
                    System.out.println("Staring load aware sheduling in Heron...");
                    schedulerController.basedWeightSchedule(packingPlan);

                    // re-initial these value
                    count = 0;
                    lastRescheduling = System.currentTimeMillis();

                    // 20180714 add: stop thread after update topology only one time in current implementation
                    isStart = false;
                }

                List<Node> nodeList = DataManager.getInstance().getLoadOfNode();
                int differentPercentage = DataManager.getInstance().calculateDifferentLoadForTrigger(nodeList);
                if (differentPercentage > RESCHEDULING_THRESHOLD) {
                    FileUtils.writeToFile(FILE_NAME, "[SCHEDULE-THREAD] - Current different percentage is: " + differentPercentage + ". And the count is: " + count + "!!!");
                    count += 1;
                } else {
                    count = 0;
                }
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
