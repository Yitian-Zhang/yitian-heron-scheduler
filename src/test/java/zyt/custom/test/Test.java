package zyt.custom.test;

import org.apache.log4j.Logger;
import zyt.custom.my.scheduler.FileUtils;
import zyt.custom.my.scheduler.Utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {

    private static Logger logger = Logger.getLogger(Test.class);
    private static String filename = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";

    private static void loggerTest() {
//        logger.info("this is info log ...");
//        logger.debug("this is debug log ...");
    }

    // get process id
    private static String getProcessId() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        System.out.println("JVM name is: " + runtimeMXBean.getName());
        System.out.println(runtimeMXBean.getVmName());
        return runtimeMXBean.getName().split("@")[0];
    }

    private static long getLoad(long threadId, Map<Long, List<Long>> mapList) {

        long total = 0;
        List<Long> loadData = mapList.get(threadId);
        for (long load : loadData)
            total += load;
        return total / (loadData.size() * 10);
    }

    private static void getLoadTest() {
        List<Long> list = new ArrayList<>();
        list.add(100l);
        list.add(100l);
        list.add(100l);
        Map<Long, List<Long>> mapList = new HashMap<>();
        mapList.put(1l, list);
        long result = getLoad(1l, mapList);
        System.out.println("result: " + result);

        for (long item : mapList.keySet()) {
            System.out.println("item result: " + Utils.collectionToString(mapList.get(item)));
        }
    }

    private static String getCurrentTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = df.format(System.currentTimeMillis());
        return currentTime;
    }

    private static void testLogFiles() {
        String filename = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";
        FileUtils.writeToFile(filename, "Starting onSchedule...");
    }

    private static String getString() {
        return null;
    }

    private static String getSting2() {
        return "";
    }

    private static void listTest() {
        List<Integer> list = new ArrayList<>();
        list.add(3);
        list.add(2);
        list.add(4);
        System.out.println("LIst size is:" + list.size());

        for (int item : list) {
            System.out.println(item);
        }

        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }

//        for (int item : list) {
//            list.remove(item);
//        }

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == 4) {
                list.remove(list.get(i));
            }
        }
        System.out.println("LIst size is:" + list.size());
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }

    private static void unassignmentTaskTest() {
        List<Integer> unAssignmentTaskList = new ArrayList<>();
        unAssignmentTaskList.add(4);
        unAssignmentTaskList.add(5);
//        unAssignmentTaskList.add(1);
//        unAssignmentTaskList.add(3);
//        unAssignmentTaskList.add(5);

        Map<String, List<Integer>> newTaskToStmgr = new HashMap<>();
        List<Integer> stmgrTaskList1 = new ArrayList<>();
        stmgrTaskList1.add(7);
        stmgrTaskList1.add(8);
        newTaskToStmgr.put("stmgr-1", stmgrTaskList1);

        List<Integer> stmgrTaskList2 = new ArrayList<>();
        stmgrTaskList2.add(1);
        stmgrTaskList2.add(6);
        stmgrTaskList2.add(3);
        stmgrTaskList2.add(2);
        newTaskToStmgr.put("stmgr-2", stmgrTaskList2);

//        List<Integer> stmgrTaskList1 = new ArrayList<>();
////        stmgrTaskList1.add(7);
////        stmgrTaskList1.add(8);
//        newTaskToStmgr.put("stmgr-1", stmgrTaskList1);
//
//        List<Integer> stmgrTaskList2 = new ArrayList<>();
//        stmgrTaskList2.add(2);
//        stmgrTaskList2.add(4);
////        stmgrTaskList2.add(3);
////        stmgrTaskList2.add(2);
//        newTaskToStmgr.put("stmgr-2", stmgrTaskList2);


        for (String stmgrId : newTaskToStmgr.keySet()) {
            System.out.println("Now ->" + stmgrId);
            List<Integer> temp = new ArrayList<>();
            int avilableTaskNum = 4 - newTaskToStmgr.get(stmgrId).size(); // still avilable task num of this stmgr
            int unassignmentTasks = unAssignmentTaskList.size();
            System.out.println("Avaliable task num is:" + avilableTaskNum + ", unassignment task num is: " + unassignmentTasks);
            if (unassignmentTasks <= avilableTaskNum) { // if avilable task num >= unassignment task num
                for (int i = 0; i < unAssignmentTaskList.size(); i++) { // all task
                    System.out.println("Now, assignment: " + unAssignmentTaskList.get(i));
                    newTaskToStmgr.get(stmgrId).add(unAssignmentTaskList.get(i));
//                    unAssignmentTaskList.remove(unAssignmentTaskList.get(i)); // has assigned and then remove from unassignemnt task list
                    temp.add(unAssignmentTaskList.get(i));
                }
                unAssignmentTaskList.removeAll(temp);
            } else {
                for (int i = 0; i < avilableTaskNum; i++) {
                    System.out.println("Now, assignment: " + unAssignmentTaskList.get(i));
                    newTaskToStmgr.get(stmgrId).add(unAssignmentTaskList.get(i)); // add aviable num to this stmgr
//                    unAssignmentTaskList.remove(unAssignmentTaskList.get(i));
                    temp.add(unAssignmentTaskList.get(i));
                }
                unAssignmentTaskList.removeAll(temp);
            }
        }
    }

    private static void formatDouble() {
        double f = 111231.5585;

        BigDecimal bg = new BigDecimal(f);
        double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println(f1);

        // DecimalFormat转换最简便
        DecimalFormat df = new DecimalFormat("#.00");
        System.out.println(df.format(f));
        FileUtils.writeToFile(filename, "DecimalFormat: " + df.format(f));

        //String.format打印最简便
        System.out.println(String.format("%.2f", f));
        FileUtils.writeToFile(filename, "StringFormat: " + String.format("%.2f", f));

    }


    public static void main(String[] args) {

        // get current Process id
//        String currentJVMProcessId = getProcessId();
//        System.out.println("Current JVM Process id is: " + currentJVMProcessId);
        // output:
//        JVM name is: 4434@heron01
//        Java HotSpot(TM) 64-Bit Server VM
//        Current JVM Process id is: 4434

//        System.out.println(getCurrentTime());
//        System.out.println(getCurrentTime());
//        try {
//            Thread.sleep(2*1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        System.out.println(getCurrentTime());
//        System.out.println(getCurrentTime());
//        String topologyId = "LocalSentenceWordCountTopology10ec4b32-4482-4825-b7ba-621f82dd5309";
//        try {
//            List<ExecutorPair> hostEdgesList = DataManager.getInstance().calculateHotEdges(topologyId);
//            for (ExecutorPair pair : hostEdgesList) {
//                System.out.println(pair);
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

//        testLogFiles();

//        String test = getString();
//        if (test==null) {
//            System.out.println("this is null");
//        }

//        unassignmentTaskTest();

        formatDouble();
    }
}
