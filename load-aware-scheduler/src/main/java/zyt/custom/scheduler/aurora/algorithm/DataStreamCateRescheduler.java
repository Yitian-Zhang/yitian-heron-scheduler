package zyt.custom.scheduler.aurora.algorithm;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.*;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import org.apache.log4j.Logger;
import zyt.custom.scheduler.Constants;
import zyt.custom.scheduler.DataManager;
import zyt.custom.scheduler.component.ExecutorPair;
import zyt.custom.scheduler.rescheduler.AuroraRescheduler;
import zyt.custom.scheduler.utils.SchedulerUtils;
import zyt.custom.scheduler.utils.TopologyInfoUtils;
import zyt.custom.utils.FileUtils;
import zyt.custom.utils.Utils;

import java.sql.SQLException;
import java.util.*;

/**
 * @author yitian
 */
public class DataStreamCateRescheduler implements AuroraRescheduler {

    private static final Logger logger = Logger.getLogger(DataStreamCateRescheduler.class.getName());

    // basic variable
    private static final String filename = Constants.SCHEDULER_LOG_FILE;;
    private Config config;
    private Config runtime;
    private TopologyAPI.Topology topology;
    private String topologyName;
    private PackingPlanProtoSerializer serializer;
    private PackingPlanProtoDeserializer deserializer;

    // addition
    private static final int TASK_NUM_PER_CONTAINER = 4;
    private ByteAmount containerRamPadding = Constants.DEFAULT_RAM_PADDING_PER_CONTAINER;


    public DataStreamCateRescheduler() {
    }

    public void initialize(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;
        this.topology = Runtime.topology(this.runtime);
        this.topologyName = Runtime.topologyName(this.runtime);
        this.serializer = new PackingPlanProtoSerializer();
        this.deserializer = new PackingPlanProtoDeserializer();

        // 2018-07-18 add for hot edge
        // this.containerRamPadding = TopologyInfoUtils.getContainerRamPadding(topology.getTopologyConfig().getKvsList());
    }

    /**
     * 2018-07-18 add
     * Hot Edge scheduler algorithm
     *
     * @param packingPlan
     */
    @Override
    public void reschedule(PackingPlan packingPlan) {
        logger.info("-----------------HOTEDGE RESCHEDULER START-----------------");
        String stateMgrClass = Context.stateManagerClass(this.config); // get state manager instance
        IStateManager stateMgr = null;

        try {
            stateMgr = ReflectionUtils.newInstance(stateMgrClass);
            FileUtils.writeToFile(filename, "Create IStateManager object success...");
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        try {
            stateMgr.initialize(this.config);
            SchedulerStateManagerAdaptor stateManagerAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);

            logger.info("Current packing algorithm is: " + Context.packingClass(config));
            // physcial plan
            logger.info("Before trigger scheduler, the physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
            TopologyInfoUtils.getPhysicalPlanInfo(physicalPlan, filename);

            // packing plan
            logger.info("Before trigger scheduler, the packing plan is:");
            TopologyInfoUtils.printPackingInfo(packingPlan, filename);

            // **************************NEED TO CUSTOM ALGORITHM START***************************
            // 201-07-18 add
            logger.info("*****************CREATE NEW PACKINGPLAN USING CUSTOM ALGORITHM*****************");
            // get executorpair list from DB, pair list have sorted by traffic desc
            List<ExecutorPair> pairList = DataManager.getInstance().getInterExecutorTrafficList(topology.getId());
            // get hot edge pair from DB
            List<ExecutorPair> hotPairList = DataManager.getInstance().calculateHotEdges(topology.getId());
            // get stmgr to host map: hostname -> list of stmgr
            Map<String, List<String>> stmgrToHostMap = getStmgrToHost(physicalPlan);
            // get taskId to stmgr map: stmgrId -> list of taskid
            Map<String, List<Integer>> taskToStmgr = getTasksToStmgr(physicalPlan);
            // get task down stream map for all task
//            Map<Integer, List<Integer>> taskDownStreamMap = getTaskStream(pairList); // useless
            // get task load map
            Map<Integer, Long> taskLoadMap = DataManager.getInstance().getTaskLoadList();

            // new task assignment allocation
            logger.info("Create new TaskToStmgr(stmgr->task list) to build new ContainerPlan....");
            Map<String, List<Integer>> newTaskToStmgr = new HashMap<>();
            for (String stmgrId : taskToStmgr.keySet()) {
                if (newTaskToStmgr.get(stmgrId) == null) {
                    newTaskToStmgr.put(stmgrId, new ArrayList<Integer>());
                }
            }

            // 20180727 add for output ------------------------------------------------------------------------
            logger.info("Starting Hot Schedule Function...");
            logger.info("HAS HOT EDGE OR NOT?!");
            if (hotPairList.size() == 0) {
                logger.info("THERE IS NO HOT EDGE AT ALL!!! DO NOT RESCHEULDE!!!");
            } else {
                logger.info("THERE IS YES HOT EDGE AT ALL!!! SIZE: " + hotPairList.size());
            }
            // ------------------------------------------------------------------------------------------------

            for (ExecutorPair pair : pairList) { // iterate all taskpair in pairList, have sorted by traffic desc
                int sourceTaskId = pair.getSource().getBeginTask();
                int destinationTaskId = pair.getDestination().getBeginTask();

                if (hotPairList.contains(pair)) { // if this edge is hot edge
                    logger.info("Current hot edge: ( " + sourceTaskId + " , " + destinationTaskId + " )");

                    // get new stmgr of this pair task
                    String newSourceStmgrId = getStmgrForTask(sourceTaskId, newTaskToStmgr);
                    String newDestionationStmgrId = getStmgrForTask(destinationTaskId, newTaskToStmgr);
                    logger.info("Current hot edge new assignment: (sourcetask in: " + newSourceStmgrId + ", destiantiontask in: " + newDestionationStmgrId + ")");

                    // get stmgr of this pair task
                    String sourceStmgrId = getStmgrForTask(sourceTaskId, taskToStmgr);
                    String destinationStmgrId = getStmgrForTask(destinationTaskId, taskToStmgr);

                    // jude this edge has assigned or not. if both of tasks has already assigned (in newTaskToStmgr), then do:
                    if (newSourceStmgrId != null && newDestionationStmgrId != null) {
                        logger.info("1-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "] both has already assigned in NewTaskToStmagr map. No need to assinged.");
                    } else if (newSourceStmgrId != null && newDestionationStmgrId == null) {
                        logger.info("2-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "], SourceTask has already assigned in NewTaskToStmagr map. Need to assgined destination task :" + destinationTaskId + "");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newSourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newSourceStmgrId).add(sourceTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newSourceStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newSourceStmgrId, newTaskToStmgr);
                            if (newSourceStmgrAvaliableTaskNum > 0) {
                                logger.info("NEW. NewSourceStmgr still has avilable task num: " + newSourceStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + newSourceStmgrId);
                                newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            } else {
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                logger.info("NEW. NewSrouceStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------
//                            newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);

                            // remove this pari from hotlist
                            logger.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            logger.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newSourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newSourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newSourceStmgrId).add(sourceTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newSourceStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newSourceStmgrId, newTaskToStmgr);
                            if (newSourceStmgrAvaliableTaskNum > 0) {
                                logger.info("NEW. NewSourceStmgr still has avilable task num: " + newSourceStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + newSourceStmgrId);
                                newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            } else {
//                                logger.info("NEW. NewSrouceStmgr do not have aviable task num. So the destination task: " +destinationTaskId+" will assigned to this original itself stmgr.");
//                                newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                // 2018-07-25 modified
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                logger.info("NEW. NewSrouceStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------
//                            newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            // remove this pari from hotlist
                            logger.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            logger.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newSourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        }

                    } else if (newSourceStmgrId == null && newDestionationStmgrId != null) {
                        logger.info("3-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "], DestinationTask has already assigned in NewTaskToStmagr map. Need to assgined destination task :" + sourceTaskId + "");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newDestionationStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(newDestionationStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newDestinationStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newDestionationStmgrId, newTaskToStmgr);
                            if (newDestinationStmgrAvaliableTaskNum > 0) {
                                logger.info("NEW. NewDestionationStmgr still has avilable task num: " + newDestinationStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                logger.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + newDestionationStmgrId);
                                newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
                            } else {
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                logger.info("NEW. NewDestionationStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------

                            // remove this pari from hotlist
                            logger.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            logger.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newDestionationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newDestionationStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(newDestionationStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newDestinationStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newDestionationStmgrId, newTaskToStmgr);
                            if (newDestinationStmgrAvaliableTaskNum > 0) {
                                logger.info("NEW. NewDestionationStmgr still has avilable task num: " + newDestinationStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                logger.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + newDestionationStmgrId);
                                newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
                            } else {
//                                logger.info("NEW. NewDestionationStmgr do not have aviable task num. So the source task: " +sourceTaskId+" will assigned to this original itself stmgr.");
//                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                logger.info("NEW. NewDestionationStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------

                            // remove this pari from hotlist
                            logger.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            logger.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newDestionationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        }

                    } else if (newSourceStmgrId == null && newDestionationStmgrId == null) {
                        logger.info("4-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "] both has already not assigned in NewTaskToStmagr map. Both need to assinged.");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + sourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                            if (newCommonStmgrAvaliableTaskNum > 0) {
                                // source task
                                logger.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                logger.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);

                                // the new task
                                newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    logger.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                    logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                    newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);
                                } else {
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    logger.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    }
                                }

                            } else {
                                // source task
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                logger.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }

                                // destination task
                                int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                if (currentMinStmgrAvaliableTaskNum > 0) {
                                    logger.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                    logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                } else {
                                    minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    logger.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    }
                                }
                            }
                            // -----------------------------------------------------------------------------------
                            // remove this pari from hotlist
                            logger.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            logger.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, sourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            logger.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            // get host load using new task to stmgr map
                            long sourceStmgrLoad = getLoadForStmgr(sourceStmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                            long destiantionStmgrLoad = getLoadForStmgr(destinationStmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                            if (sourceStmgrLoad < destiantionStmgrLoad) {
                                logger.info("-SourceStmgrLoad < DestinationStmgrLocd, so assign this pair to sourceStrmgrId...");

                                logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + sourceStmgrId);
                                // add to newtaskToStmgr: reassignment task
//                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
//                                newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);

                                // 2018-07-21 add for validate stmgr task number -------------------------------------
                                int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    // source task
                                    logger.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                    logger.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                    newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);

                                    // the new task
                                    newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                    if (newCommonStmgrAvaliableTaskNum > 0) {
                                        logger.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                        newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);
                                    } else {
                                        String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        logger.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }

                                } else {
                                    // source task
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    logger.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                    }

                                    // destination task
                                    int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                    if (currentMinStmgrAvaliableTaskNum > 0) {
                                        logger.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    } else {
                                        minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        logger.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }
                                }
                                // -----------------------------------------------------------------------------------

                                // remove this pari from hotlist
                                logger.info("Removed this hot pair from hotPairList...");
                                hotPairList.remove(pair);

                                logger.info("Now, find extra hot edge for this pair to assignment...");
                                assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, sourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);
                            } else {
                                logger.info("-SourceStmgrLoad >= DestinationStmgrLocd, so assign this pair to destinationStmgrId...");

                                logger.info("Then, assignment this host pair: " + pair + " to new packingplan: " + destinationStmgrId);
                                // add to newtaskToStmgr: reassignment task
//                                newTaskToStmgr.get(destinationStmgrId).add(sourceTaskId);
//                                newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                // 2018-07-21 add for validate stmgr task number -------------------------------------

                                int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(destinationStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    // source task
                                    logger.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                    logger.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + destinationStmgrId);
                                    newTaskToStmgr.get(destinationStmgrId).add(sourceTaskId);

                                    // the new task
                                    newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(destinationStmgrId, newTaskToStmgr);
                                    if (newCommonStmgrAvaliableTaskNum > 0) {
                                        logger.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + destinationStmgrId);
                                        newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                    } else {
                                        String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        logger.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }

                                } else {
                                    // source task
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    logger.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                    }

                                    // destination task
                                    int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                    if (currentMinStmgrAvaliableTaskNum > 0) {
                                        logger.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        logger.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    } else {
                                        minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        logger.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }
                                }
                                // -----------------------------------------------------------------------------------

                                // remove this pari from hotlist
                                logger.info("Removed this hot pair from hotPairList...");
                                hotPairList.remove(pair);

                                logger.info("Now, find extra hot edge for this pair to assignment...");
                                assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, destinationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);
                            }
                            logger.info("Finally, ending assignment for this hot pair:" + pair);
                        }
                    }
                } /* - end if this pair is hot edge*/
            } /* - end for pair list */

            logger.info("-------------------CURRENT STMGR->LIST OF TASK -START-------------------");
            List<Integer> unAssignmentTaskList = new ArrayList<>();
            // assignment other un-assignment(un-hot pair) task to newTaskToStmgr
            for (String stmgrId : taskToStmgr.keySet()) {
                List<Integer> taskList = taskToStmgr.get(stmgrId);
                for (int taskId : taskList) {
                    if (getStmgrForTask(taskId, newTaskToStmgr) == null) {
                        unAssignmentTaskList.add(taskId);
                    }
                }
            }
            logger.info("Current unassignment task list: " + Utils.collectionToString(unAssignmentTaskList));

            // based on origin allocation to assignment these task
//            logger.info("Based on origin allocation to assignment these unassignment tasks...");
//            for (int taskId : unAssignmentTaskList) {
//                if (getStmgrForTask(taskId, taskToStmgr) != null) {
//                    String stmgrId = getStmgrForTask(taskId, taskToStmgr);
//                    newTaskToStmgr.get(stmgrId).add(taskId);
//                }
//            }
            // ****************************************************************************
            logger.info("Based on host load to assigned these unassignment tasks... -undo now");
            Map<String, Long> stmgrLoadMap = new HashMap<>(); // stmgrId -> stmgr load
            for (String hostname : stmgrToHostMap.keySet()) {
                List<String> stmgrList = stmgrToHostMap.get(hostname);
//                long hostLoad = 0l;
                for (String stmgrId : stmgrList) { // only one stmgr in one hostname
                    long stmgrLoad = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
//                    hostLoad += stmgrLoad;
                    stmgrLoadMap.put(stmgrId, stmgrLoad);
                    logger.info("StmgrId: " + stmgrId + " -> load: " + stmgrLoad);
                }
            }

            logger.info("Before assigned the unassignment task, current newTaskStmger map is: ");
            outputCurrentStmgrMap(newTaskToStmgr);

            logger.info("Based on host task number to assigned these unassignment tasks... -do now");
            for (String stmgrId : newTaskToStmgr.keySet()) {
                List<Integer> temp = new ArrayList<>();
                int avilableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr); // still avilable task num of this stmgr
                int unassignmentTasks = unAssignmentTaskList.size();
                logger.info("AvilableTaskNum is: " + avilableTaskNum + ", unAassignment task num is: " + unassignmentTasks);
                if (unassignmentTasks <= avilableTaskNum) { // if avilable task num >= unassignment task num
                    for (int i = 0; i < unAssignmentTaskList.size(); i++) { // all task
                        newTaskToStmgr.get(stmgrId).add(unAssignmentTaskList.get(i));
                        temp.add(unAssignmentTaskList.get(i));
                    }
                    unAssignmentTaskList.removeAll(temp); // has assigned and then remove from unassignemnt task list
                } else {
                    for (int i = 0; i < avilableTaskNum; i++) {
                        newTaskToStmgr.get(stmgrId).add(unAssignmentTaskList.get(i));
                        temp.add(unAssignmentTaskList.get(i));
                    }
                    unAssignmentTaskList.removeAll(temp); // add aviable num to this stmgr
                }
            }

            // current simple to assign the unasiignment tasks to original stmgr location
//            for (int taskId : unAssignmentTaskList) {
//                newTaskToStmgr.get("stmgr-2").add(taskId);
//            }
            // ****************************************************************************

            logger.info("Current newTaskStmger map is: ");
            outputCurrentStmgrMap(newTaskToStmgr);
            logger.info("-------------------CURRENT STMGR->LIST OF TASK -END-------------------");

            // get new allocation
            logger.info("THEN. Get host edge allocation using newTaskToStmgr...");
            Map<Integer, List<InstanceId>> hotEdgeAllocation = getHotEdgeAllocation(newTaskToStmgr, physicalPlan);
            Set<PackingPlan.ContainerPlan> newContainerPlans = new HashSet<>();

            logger.info("Creating new containerPlan...");
            for (int containerId : hotEdgeAllocation.keySet()) {
                List<InstanceId> instanceIdList = hotEdgeAllocation.get(containerId);
                //
                Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
                // unused in this class
                ByteAmount containerRam = containerRamPadding;
                logger.info("Getting instance resource from packing plan...");
                for (InstanceId instanceId : instanceIdList) {
                    Resource resource = TopologyInfoUtils.getInstanceResourceFromPackingPlan(instanceId.getTaskId(), packingPlan);
                    if (resource != null) {
                        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
                    } else {
                        logger.info("Getting instance resource error!!!");
                    }
                }

                PackingPlan.ContainerPlan newContainerPlan = null;
                logger.info("Get container resource from packing plan...");
                Resource containerRequiredResource = TopologyInfoUtils.getContainerResourceFromPackingPlan(containerId, packingPlan);
                if (containerRequiredResource != null) {
                    newContainerPlan = new PackingPlan.ContainerPlan(containerId, new HashSet<>(instancePlanMap.values()), containerRequiredResource);
                } else {
                    logger.info("Getting container resource error!!!");
                }
                newContainerPlans.add(newContainerPlan);
            }

            logger.info("Creating new hot packing plan...");
            PackingPlan hotPackingPlan = new PackingPlan(topology.getId(), newContainerPlans);
            logger.info("Validate Packing plan...");
            SchedulerUtils.validatePackingPlan(hotPackingPlan);

            logger.info("Now, Created hotPackingPlan successed, hot packing plan info is: ");
            TopologyInfoUtils.printPackingInfo(hotPackingPlan, filename);

            logger.info("Now, Update topology with new packing by updateTopologyManager...");
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(hotPackingPlan);

            // **************************NEED TO CUSTOM ALGORITHM END***************************

            Config newRuntime = Config.newBuilder()
                    .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
                    .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateManagerAdaptor)
                    .build();

            // Create a ISchedulerClient basing on the config
            ISchedulerClient schedulerClient = SchedulerUtils.getSchedulerClient(newRuntime, config);

            // build updatetopologyrequest object to update topogolgy
            Scheduler.UpdateTopologyRequest updateTopologyRequest =
                    Scheduler.UpdateTopologyRequest.newBuilder()
                            .setCurrentPackingPlan(currentPackingPlan)
                            .setProposedPackingPlan(proposedPackingPlan)
                            .build();

            logger.info("Now, Update topology using updateTopologyManager...");
            logger.info("Sending Updating Topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            // Clean the connection when we are done.
            logger.info("Schedule update topology successfully!!!");
            logger.info("After trigger scheduler, the physical plan is:");
            TopologyInfoUtils.getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName), filename);

            logger.info("-----------------HOTEDGE RESCHEDULER END-----------------");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // close zookeeper client connnection
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }

    /****************************************************************************/
    //                  Hot Edge Reschedule Tools function Start
    /****************************************************************************/

    private Map<String, List<String>> getStmgrToHost(PhysicalPlans.PhysicalPlan physicalPlan) {
        logger.info("[FUNCTION]--------------GET HOSTNAME -> STMGRS LIST START--------------");
        logger.info("Getting hostname from physical plan...");
        List<String> hostList = new ArrayList<>(); // hostname list in this topology
        for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) {
            String hostname = stMgr.getHostName();
            if (!hostList.contains(hostname)) {
                hostList.add(hostname);
            }
        }
        logger.info("Hostlist: " + Utils.collectionToString(hostList));
        // hostname -> list of stmgrid
        logger.info("Getting host -> stmgr list from physical plan...");
        Map<String, List<String>> stmgrToHostMap = new HashMap<>();
        for (String hostname : hostList) {
            List<String> stmgrList = null;
            if (stmgrToHostMap.get(hostname) == null) {
                stmgrList = new ArrayList<>();
                stmgrToHostMap.put(hostname, stmgrList);
            }
            for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) {
                if (stMgr.getHostName().equals(hostname)) {
                    stmgrList.add(stMgr.getId()); //
                }
            }
            stmgrToHostMap.put(hostname, stmgrList);
            logger.info("hostname: " + hostname + " stmgr list: " + Utils.collectionToString(stmgrToHostMap.get(hostname)));
        }
        logger.info("[FUNCTION]--------------GET HOSTNAME -> STMGRS LIST END--------------");
        return stmgrToHostMap;
    }

    private Map<String, List<Integer>> getTasksToStmgr(PhysicalPlans.PhysicalPlan physicalPlan) {
        logger.info("[FUNCTION]------------GET STMGR -> TASKS LIST START------------");
        logger.info("Getting stmgr -> task list from physical plan...");
        Map<String, List<Integer>> taskToContainerMap = new HashMap<>();
        for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) {
            List<Integer> taskList = null;
            if (taskToContainerMap.get(stMgr.getId()) == null) {
                taskList = new ArrayList<>();
                taskToContainerMap.put(stMgr.getId(), taskList);
            }
            for (PhysicalPlans.Instance instance : physicalPlan.getInstancesList()) {
                if (instance.getStmgrId().equals(stMgr.getId())) {
                    taskList.add(instance.getInfo().getTaskId());
                }
            }
            taskToContainerMap.put(stMgr.getId(), taskList);
            logger.info("Stmgr id: " + stMgr.getId() + " instance list is: " + taskToContainerMap.get(stMgr.getId()));
        }
        logger.info("[FUNCTION]------------GET STMGR -> TASKS LIST END------------");
        return taskToContainerMap;
    }

    private boolean judgeTaskOnSameStmgr(ExecutorPair pair, Map<String, List<Integer>> taskToStmgr) {
        logger.info("Invoking...Judging task on the same container or not...");
        int sourceTaskId = pair.getSource().getBeginTask();
        int destinationTaskId = pair.getDestination().getBeginTask();

        for (String stmgrId : taskToStmgr.keySet()) {
            List<Integer> taskListOnStmgr = taskToStmgr.get(stmgrId);
            if (taskListOnStmgr.contains(sourceTaskId) && taskListOnStmgr.contains(destinationTaskId)) {
                return true; // if on the same container, return true
            }
        }
        return false;
    }

    private String getStmgrForTask(int taskId, Map<String, List<Integer>> taskToStmgr) {
        String stmgrIdResult = null;
        for (String stmgrId : taskToStmgr.keySet()) {
            List<Integer> taskListOnStmgr = taskToStmgr.get(stmgrId);
            if (taskListOnStmgr.contains(taskId)) {
                stmgrIdResult = stmgrId;
                break;
            }
        }
        return stmgrIdResult;
    }

    private List<ExecutorPair> getExtraHotEdgeForSourceTask(int sourceHotTaskId, List<ExecutorPair> hotPairList) {
        List<ExecutorPair> extraHotPairList = new ArrayList<>();
        for (ExecutorPair pair : hotPairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if (sourceHotTaskId == destinationTaskId) { // m -> i is hot edge
                extraHotPairList.add(pair);
                logger.info("Source taskid: [" + sourceHotTaskId + "], has extra hot edge: " + pair);
            }
        }
        return extraHotPairList;
    }

    private List<ExecutorPair> getExtraHotEdgeForDestinationTask(int destinationHotTaskId, List<ExecutorPair> hotPairList) {
        List<ExecutorPair> extraHotPairList = new ArrayList<>();
        for (ExecutorPair pair : hotPairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if (destinationHotTaskId == sourceTaskId) { // j -> n is hot edge
                extraHotPairList.add(pair);
                logger.info("Destination taskid: [" + destinationTaskId + "], has extra hot edge: " + pair);
            }
        }
        return extraHotPairList;
    }

    /**
     * Calculate stmgr load using arguments as follows:
     *
     * @param stmgrId:        stmgId
     * @param stmgrToHostMap: hostname -> list stmgr
     * @param newTaskToStmgr: proposed stmgrId -> list task
     * @param taskLoadMap:    task -> load
     * @return load of this stmgr
     */
    private long getLoadForStmgr(String stmgrId, Map<String, List<String>> stmgrToHostMap, Map<String, List<Integer>> newTaskToStmgr, Map<Integer, Long> taskLoadMap) {
        logger.info("[FUNCTION]------------GET STMGR CURRENT LOAD START------------");
        long stmgrLoad = 0l;
        String currentHostname = "";
        logger.info("Get hostname of this stmger: " + stmgrId);
        for (String hostname : stmgrToHostMap.keySet()) {
            List<String> stmgrList = stmgrToHostMap.get(hostname);
            for (String stmgrItem : stmgrList) {
                if (stmgrId.equals(stmgrItem)) {
                    currentHostname = hostname;
                    break;
                } else {
                    logger.info("Not find the stgmrID: " + stmgrId);
                }
            }
        }
        logger.info("Calculate stmgr load of current host: " + currentHostname);
        List<Integer> taskList = newTaskToStmgr.get(stmgrId);
        for (Integer taskId : taskList) {
            long taskLoad = taskLoadMap.get(taskId);
            logger.info(taskId + " 's load is: " + taskLoad);
            stmgrLoad += taskLoad;
        }
        logger.info("[FUNCTION]------------GET STMGR CURRENT LOAD END------------");
        return stmgrLoad;
    }

    private Map<Integer, List<InstanceId>> getHotEdgeAllocation(Map<String, List<Integer>> taskToStmgr, PhysicalPlans.PhysicalPlan physicalPlan) {
        logger.info("[FUNCTION]----------CREAT NEW HOT ALLOCATION USING TASKTOSTMGR START----------");
        Map<Integer, List<InstanceId>> allocation = new HashMap<>();
        int numContainer = TopologyUtils.getNumContainers(topology);
        int totalInstance = TopologyUtils.getTotalInstance(topology);
        List<PhysicalPlans.Instance> instanceList = getInstancesList(physicalPlan);

        if (numContainer > totalInstance) {
            throw new RuntimeException("More containers allocated than instances.");
        }
        for (int i = 1; i <= numContainer; i++) {
            allocation.put(i, new ArrayList<>());
        }

        for (String stmgrId : taskToStmgr.keySet()) {
            int containerId = Integer.valueOf(stmgrId.split("-")[1]);
            InstanceId instanceId = null;
            List<Integer> taskList = taskToStmgr.get(stmgrId);
            for (int taskId : taskList) { // iterate this stmgrid -> list of task
                instanceId = getInstanceInfoByTask(taskId, instanceList);
                if (instanceId != null) {
                    allocation.get(containerId).add(instanceId);
                }
            }
            logger.info("Container: " + containerId + " has new allocation: " + Utils.collectionToString(allocation.get(containerId)));
        }
        logger.info("[FUNCTION]----------CREAT NEW HOT ALLOCATION USING TASKTOSTMGR END----------");
        return allocation;
    }

    private List<PhysicalPlans.Instance> getInstancesList(PhysicalPlans.PhysicalPlan physicalPlan) {
        logger.info("Getting instance list from physical plan...");
        List<PhysicalPlans.Instance> instanceList = physicalPlan.getInstancesList();
        return instanceList;
    }

    private InstanceId getInstanceInfoByTask(int taskId, List<PhysicalPlans.Instance> instanceList) {
//        List<PhysicalPlans.Instance> instanceList = getInstancesList();
        InstanceId instanceId = null;
        for (PhysicalPlans.Instance instance : instanceList) {
            if (instance.getInfo().getTaskId() == taskId) {
                instanceId = new InstanceId(instance.getInfo().getComponentName(), instance.getInfo().getTaskId(), instance.getInfo().getComponentIndex());
            }
        }
        return instanceId;
    }

    /**
     * Find extra hot edge and assigned them to target stmgrId
     * 2018-07-20
     *
     * @param sourceTaskId      source task
     * @param destinationTaskId destination task
     * @param stmgrId           target stmgr
     * @param taskToStmgr       stmgrId -> list of tasks
     * @param hotPairList       list of hot edge
     * @param newTaskToStmgr    new stmgrId -> list of tasks (proposed packingplan)
     */
    private void assignmentExtraHotEdgeForPair(int sourceTaskId, int destinationTaskId, String stmgrId, Map<String, List<Integer>> taskToStmgr, List<ExecutorPair> hotPairList, Map<String, List<Integer>> newTaskToStmgr) {
        logger.info("------------------ASSIGNMENT EXTRA HOT EDGE FOR PAIR -START------------------");

        // find extra hot edge in hot pari list of this task
        logger.info("Getting extra task for this source task in this hot edge...");
        List<ExecutorPair> extraSourceHotPairList = getExtraHotEdgeForSourceTask(sourceTaskId, hotPairList);
        for (ExecutorPair extraPair : extraSourceHotPairList) {
            int extraHotSourceTaskId = extraPair.getSource().getBeginTask();

            // ## yitian added for ADT
            String hasAssignedForExtraHotSourceTask = getStmgrForTask(extraHotSourceTaskId, newTaskToStmgr);
            if (hasAssignedForExtraHotSourceTask == null) { // this extra hot source task has not assigned
                // move extra taskid to current source task stmgr
                String extraStmgrId = getStmgrForTask(extraHotSourceTaskId, taskToStmgr); // extra task stmgrid
                logger.info("The extra task of source task is: " + extraHotSourceTaskId + ", it's stmgrId: " + extraStmgrId);

                if (judgeTaskOnSameStmgr(extraPair, taskToStmgr)) { // if extra task is stll on the same stmgr with current taskid
                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        logger.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraHotSourceTaskId);
                        logger.info("The extra hot task: " + extraHotSourceTaskId + " is still on the same stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
                        logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            logger.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraHotSourceTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------
                } else {
                    logger.info("Current assignment of this extra pair is: ( " + sourceTaskId + ": " + stmgrId + ", " + extraHotSourceTaskId + ": " + extraStmgrId + " ) ");
                    logger.info("These task on different container... We should reassignment this extra task to: " + stmgrId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        logger.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraHotSourceTaskId);
                        logger.info("The extra hot task: " + extraHotSourceTaskId + " has assigned to stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
//                    logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned original itself stmgr.");
//                    newTaskToStmgr.get(extraStmgrId).add(extraHotSourceTaskId);
                        logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            logger.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraHotSourceTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------
                }
            } else {
                logger.info("The extra source task has assigned in newTaskToStmgr: " + hasAssignedForExtraHotSourceTask);
            }


            // 2018-07-27 add for bug ---------------------------------------
            // remove this pari from hotlist
            logger.info("Removed this hot pair from hotPairList...");
            hotPairList.remove(extraPair);
            // --------------------------------------------------------------
        }

        logger.info("Getting extra task for this destination task in this hot edge..");
        List<ExecutorPair> extraDestinationHotPairList = getExtraHotEdgeForDestinationTask(destinationTaskId, hotPairList);
        for (ExecutorPair extraPair : extraDestinationHotPairList) {
            int extraDestinationTaskId = extraPair.getDestination().getBeginTask();

            // ## yitian added for ADT
            String hasAssignedForExtraHotDestinationTask = getStmgrForTask(extraDestinationTaskId, newTaskToStmgr);
            if (hasAssignedForExtraHotDestinationTask == null) { // this extra hot source task has not assigned
                String extraStmgrId = getStmgrForTask(extraDestinationTaskId, taskToStmgr);
                logger.info("The extra task of destination task is: " + extraDestinationTaskId + ", It's stmgrId: " + extraStmgrId);

                if (judgeTaskOnSameStmgr(extraPair, taskToStmgr)) {
//                    newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
//                    logger.info("The extra hot task: " + extraDestinationTaskId + " is still on the same stmgr with current destination task: " + destinationTaskId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        logger.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
                        logger.info("The extra hot task: " + extraDestinationTaskId + " is still on the same stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
                        logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            logger.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraDestinationTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------

                } else {
                    logger.info("Current assignment of this extra pair is: ( " + destinationTaskId + ": " + stmgrId + ", " + extraDestinationTaskId + ": " + extraStmgrId + " ) ");
                    logger.info("These task on different container... We should reassignment this extra task to: " + stmgrId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        logger.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
                        logger.info("The extra hot task: " + extraDestinationTaskId + " has assigned to stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
//                    logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned original itself stmgr.");
//                    newTaskToStmgr.get(extraStmgrId).add(extraDestinationTaskId);
                        logger.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            logger.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraDestinationTaskId);
                        }
                    }
                    // -----------------------------------------------------------------------------
                }
            } else {
                logger.info("The extra destination task has assigned in newTaskToStmgr: " + hasAssignedForExtraHotDestinationTask);
            }

            // 2018-07-27 add for bug ---------------------------------------
            // remove this pari from hotlist
            logger.info("Removed this hot pair from hotPairList...");
            hotPairList.remove(extraPair);
            // --------------------------------------------------------------
        }
        logger.info("------------------ASSIGNMENT EXTRA HOT EDGE FOR PAIR -END------------------");
    }

    private int getTaskNumInNewStmgr(String stmgrId, Map<String, List<Integer>> newTaskToStmgr) {
        int resultCount = 0;
        List<Integer> taskListOfStmgr = newTaskToStmgr.get(stmgrId);
        if (taskListOfStmgr.size() != 0) {
            resultCount = taskListOfStmgr.size();
        }
        return resultCount;
    }

    private String getMinTaskNumOfNewStmgr(Map<String, List<Integer>> newTaskToStmgr) {
        int minTaskCount = Integer.MAX_VALUE;
        String minStmgrId = null;
        for (String stmgrId : newTaskToStmgr.keySet()) {
            int stmgrItemCount = newTaskToStmgr.get(stmgrId).size();
            if (minTaskCount > stmgrItemCount) {
                minTaskCount = stmgrItemCount;
                minStmgrId = stmgrId;
            }
        }
        return minStmgrId;
    }

    private void outputCurrentStmgrMap(Map<String, List<Integer>> newTaskToStmgr) {
        logger.info("---------------CURRENT NEW STMGR->TASK MAP START---------------");
        for (String stmgrId : newTaskToStmgr.keySet()) {
            List<Integer> taskList = newTaskToStmgr.get(stmgrId);
            logger.info("StmgrId: " + stmgrId + " -> " + Utils.collectionToString(taskList));
        }
        logger.info("---------------CURRENT NEW STMGR->TASK MAP END---------------");

    }
    /****************************************************************************/
    //                  Hot Edge Reschedule Tools function End
    /****************************************************************************/

}
