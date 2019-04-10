package zyt.custom.my.scheduler.aurora;

import com.twitter.heron.spi.packing.PackingPlan;
import zyt.custom.my.scheduler.DataManager;
import zyt.custom.my.scheduler.FileUtils;
import zyt.custom.my.scheduler.Node;

import java.sql.SQLException;
import java.util.List;

/**
 * ****************************************
 * 2018-07-04 add for:
 * AuroraCustomScheduler
 * AuroraSchedulerController
 * - TriggerSchedule
 * ****************************************
 */
public class AuroraSchedulerThread extends Thread {

    private String filename = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";
    private long lastRescheduling;
    private int count = 0;
    private PackingPlan packingPlan;
    private AuroraSchedulerController schedulerController;
    private String topologyName;
    private boolean isStart = true;

    // #20181117 for trigger rescheduling of Load-aware scheduling
    private static final int RESCHEDULING_THRESHOLD = 20;

    public AuroraSchedulerThread() {}

    public AuroraSchedulerThread(String topologyName, PackingPlan packingPlan, AuroraSchedulerController schedulerController) {
        this.topologyName = topologyName;
        this.packingPlan = packingPlan;
        this.schedulerController = schedulerController;
    }

    // 20181117 original trigger conditions backup
//    @Override
//    public void run() {
//        while (isStart) {
//            try {
//                Thread.sleep(10 * 1000);
//                FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Starting [NO. " + count + 1 + " times] WorkNode load searching...");
//                // modified at: 0722, from 18->28
//                // modified at: 0726, 28-> 34
//                // modified at: 0728, 34->39
//                if (count >= 30) { // 18 * 10s / default: 28*10s for dsc-heron
//                    // invoke trigger function
//                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Starting this time Rescheduling...");
//                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Have past 180s, last schedule time is: " + lastRescheduling + ". Triggerring scheduling...");
//                    // *******************CUSTOM SCHEDULER INVOKED*********************
//                    // invoked trigerSchedule function in AuroraSchedulerController class
////                    schedulerController.triggerSchedule(packingPlan);
//                    // invoking DSC-Heron
//                    schedulerController.hotEdgeSchedule(packingPlan);
//                    // invoking BW-Heron
////                    schedulerController.basedWeightSchedule(packingPlan);
//                    // invoke test-schedule
////                    schedulerController.testSchedule(packingPlan);
//                    // ****************************************************************
//                    // 2018-09-21 deleted for bw algorithm
////                    if (!DataManager.getInstance().calculateHotEdges(topologyName).isEmpty()) {
//////                                hotEdgeSchedule(packingPlan);
////                    } else {
////                        FileUtils.writeToFile(filename, "Now, there is no hot-edge, Waiting another 180s...");
////                    }
//                    count = 0;
//                    lastRescheduling = System.currentTimeMillis();
//                    isStart = false; // 0714 add: stop thread after update topology one time
//                }
////                List<Node> nodeList = DataManager.getInstance().getOverloadedNodes();
////                if (nodeList.size() != 0) { // exist over loaded node in cluster
////                    for (Node node : nodeList) {
////                        FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] -Overloaded node: " + node + " has overloaded :" + count + 1 + " times!!!");
////                    }
////                } else {
////                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] -No node is overloaded...");
////                }
//                count++;
//            } catch (SQLException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    // 20181117 new trigger conditions build
    @Override
    public void run() {
        // firstly sleep 120s for starting the cluster
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

                if (count >= 6) { // if continue to 60s, the different percentage > 20% (reschedling threshold), trigger the rescheduling
                    // invoke trigger function
                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Starting Rescheduling...");
                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Last schedule time is: " + lastRescheduling + ".");

                    // Triggered the rescheduling...
                    System.out.println("Staring load aware sheduling in Heron...");
                    schedulerController.basedWeightSchedule(packingPlan);

                    // re initial these value
                    count = 0;
                    lastRescheduling = System.currentTimeMillis();
                    isStart = false; // 0714 add: stop thread after update topology only one time
                }
                List<Node> nodeList = DataManager.getInstance().getLoadOfNode();
                int differentPercentage = DataManager.getInstance().calculateDifferentLoadForTrigger(nodeList);
                if (differentPercentage > RESCHEDULING_THRESHOLD) {
                    FileUtils.writeToFile(filename, "[SCHEDULE-THREAD] - Current different percentage is: " + differentPercentage + ". And the count is: " + count + "!!!");
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
