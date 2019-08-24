package zyt.custom.my.scheduler.aurora;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.*;
import com.twitter.heron.spi.scheduler.SchedulerException;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import zyt.custom.my.scheduler.DataManager;
import zyt.custom.my.scheduler.ExecutorPair;
import zyt.custom.my.scheduler.FileUtils;
import zyt.custom.my.scheduler.Utils;
import zyt.custom.my.scheduler.heron.Instance;
import zyt.custom.tools.UtilFunctions;

import javax.xml.crypto.Data;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;

/**
 * ****************************************
 * 2018-07-03 add for reschedle topo
 * 2018-07-06 noted:
 * is not successfully for now
 * detailing exception at POST about this
 * 2018-07-18 noted:
 * sovled reschedule core problem
 * ****************************************
 */
public class AuroraSchedulerController {
    private static final Logger LOG = Logger.getLogger(AuroraSchedulerController.class.getName());
    // 2018-07-18 add for hotedge ------------------------------------
    private static final ByteAmount DEFAULT_RAM_PADDING_PER_CONTAINER = ByteAmount.fromGigabytes(2); // 默认的每个container的padding ram值为2
    private static final ByteAmount MIN_RAM_PER_INSTANCE = ByteAmount.fromMegabytes(192); // MIN_RAM_PER_INSTANCE = 192m
    // 2018-07-21 add for improved aglorithm
    private static final int TASK_NUM_PER_CONTAINER = 4;
    private TopologyAPI.Topology topology;
    private String topologyName;
    //    private static SchedulerStateManagerAdaptor stateManagerAdaptor;
    private Config config;
    private Config runtime;
    private String filename = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";
    private PackingPlanProtoSerializer serializer;
    private PackingPlanProtoDeserializer deserializer;
    private ByteAmount containerRamPadding = DEFAULT_RAM_PADDING_PER_CONTAINER;
    // ---------------------------------------------------------------

    public AuroraSchedulerController(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;
        this.topology = Runtime.topology(this.runtime);
        this.topologyName = Runtime.topologyName(this.runtime);
        this.serializer = new PackingPlanProtoSerializer();
        this.deserializer = new PackingPlanProtoDeserializer();

        // 2018-07-18 add for hot edge ----------------------------------------
//        containerRamPadding = getContainerRamPadding(topology.getTopologyConfig().getKvsList());
        // --------------------------------------------------------------------
    }

    /**
     * trigger schedule function
     *
     * @param packingPlan current packing plan
     */
    public void triggerSchedule(PackingPlan packingPlan) {
        FileUtils.writeToFile(filename, "-----------------TRIGGER RESCHEDULER START-----------------");
        String stateMgrClass = Context.stateManagerClass(this.config); // get state manager instance
        IStateManager stateMgr = null;
        AuroraCustomScheduler customScheduler = null;

        try {
            stateMgr = ReflectionUtils.newInstance(stateMgrClass);
            FileUtils.writeToFile(filename, "Create IStateManager object success...");
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        try {
            stateMgr.initialize(this.config);
            SchedulerStateManagerAdaptor stateManagerAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);

            FileUtils.writeToFile(filename, "Current packing algorithm is: " + Context.packingClass(config));

            // physcial plan
            FileUtils.writeToFile(filename, "Before trigger scheduler, the physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
            getPhysicalPlanInfo(physicalPlan);

            // packing plan
            FileUtils.writeToFile(filename, "Before trigger scheduler, the packing plan is:");
            outputPackingInfoToScheduleLog(packingPlan);

            // **************************NEED TO CUSTOM ALGORITHM START***************************
            // config and runtime
            FileUtils.writeToFile(filename, "Create the new Config using build new Config object with put FFDP packing algorithm...");
            // 2018-07-03 add : ffdp algorithm resources is not avaliable
            config = Config.newBuilder().putAll(config).put(Key.PACKING_CLASS, "com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking").build();
//        config = Config.newBuilder().putAll(config).put(Key.PACKING_CLASS, "com.twitter.heron.packing.roundrobin.RoundRobinPacking").build();
//            outputRuntimeInfo(config);

            String packingClass = Context.packingClass(config);
            FileUtils.writeToFile(filename, "Then, packing algorithm is: " + packingClass); // there is no problem

            // new packing plan
            FileUtils.writeToFile(filename, "Create the new PackingPlan using New Packing Algorithm...");
            PackingPlan newPackingPlan = LauncherUtils.getInstance().createPackingPlan(config, runtime);
            FileUtils.writeToFile(filename, "Then, new packing plan info...");
            outputPackingInfoToScheduleLog(newPackingPlan);

            // serializer packing plan for creating updateTopologyRequest
            FileUtils.writeToFile(filename, "Now, Update topology using updateTopologyManager...");
//            PackingPlans.PackingPlan currentPackingPlan = serializer.toProto(packingPlan);
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(newPackingPlan);
            // **************************NEED TO CUSTOM ALGORITHM END***************************

            // 2018-07-18 add ------------------------------------------
//            validateRuntimeManage(stateManagerAdaptor, topologyName);
            // ---------------------------------------------------------

            Config newRuntime = Config.newBuilder()
                    .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
                    .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateManagerAdaptor)
                    .build();

            // Create a ISchedulerClient basing on the config
            ISchedulerClient schedulerClient = getSchedulerClient(newRuntime);

            // build updatetopologyrequest object to update topogolgy
            Scheduler.UpdateTopologyRequest updateTopologyRequest =
                    Scheduler.UpdateTopologyRequest.newBuilder()
                            .setCurrentPackingPlan(currentPackingPlan)
                            .setProposedPackingPlan(proposedPackingPlan)
                            .build();

            //
            FileUtils.writeToFile(filename, "Sending Updating Topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            // Clean the connection when we are done.
            FileUtils.writeToFile(filename, "Schedule update topology successfully!!!");
            FileUtils.writeToFile(filename, "After trigger scheduler, the physical plan is:");
            getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName));

            FileUtils.writeToFile(filename, "-----------------TRIGGER RESCHEDULER END-----------------");
        } finally {
            // close zookeeper client connnection
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }

    /**
     * 2018-07-18 add
     * Hot Edge scheduler algorithm
     *
     * @param packingPlan
     */
    public void hotEdgeSchedule(PackingPlan packingPlan) throws SQLException {
        LOG.info("-----------------HOTEDGE RESCHEDULER START-----------------");
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

            LOG.info("Current packing algorithm is: " + Context.packingClass(config));
            // physcial plan
            LOG.info("Before trigger scheduler, the physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
            getPhysicalPlanInfo(physicalPlan);

            // packing plan
            LOG.info("Before trigger scheduler, the packing plan is:");
            outputPackingInfoToScheduleLog(packingPlan);

            // **************************NEED TO CUSTOM ALGORITHM START***************************
            // 201-07-18 add
            LOG.info("*****************CREATE NEW PACKINGPLAN USING CUSTOM ALGORITHM*****************");
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
            LOG.info("Create new TaskToStmgr(stmgr->task list) to build new ContainerPlan....");
            Map<String, List<Integer>> newTaskToStmgr = new HashMap<>();
            for (String stmgrId : taskToStmgr.keySet()) {
                if (newTaskToStmgr.get(stmgrId) == null) {
                    newTaskToStmgr.put(stmgrId, new ArrayList<Integer>());
                }
            }

            // 20180727 add for output ------------------------------------------------------------------------
            LOG.info("Starting Hot Schedule Function...");
            LOG.info("HAS HOT EDGE OR NOT?!");
            if (hotPairList.size() == 0) {
                LOG.info("THERE IS NO HOT EDGE AT ALL!!! DO NOT RESCHEULDE!!!");
            } else {
                LOG.info("THERE IS YES HOT EDGE AT ALL!!! SIZE: " + hotPairList.size());
            }
            // ------------------------------------------------------------------------------------------------

            for (ExecutorPair pair : pairList) { // iterate all taskpair in pairList, have sorted by traffic desc
                int sourceTaskId = pair.getSource().getBeginTask();
                int destinationTaskId = pair.getDestination().getBeginTask();

                if (hotPairList.contains(pair)) { // if this edge is hot edge
                    LOG.info("Current hot edge: ( " + sourceTaskId + " , " + destinationTaskId + " )");

                    // get new stmgr of this pair task
                    String newSourceStmgrId = getStmgrForTask(sourceTaskId, newTaskToStmgr);
                    String newDestionationStmgrId = getStmgrForTask(destinationTaskId, newTaskToStmgr);
                    LOG.info("Current hot edge new assignment: (sourcetask in: " + newSourceStmgrId + ", destiantiontask in: " + newDestionationStmgrId + ")");

                    // get stmgr of this pair task
                    String sourceStmgrId = getStmgrForTask(sourceTaskId, taskToStmgr);
                    String destinationStmgrId = getStmgrForTask(destinationTaskId, taskToStmgr);

                    // jude this edge has assigned or not. if both of tasks has already assigned (in newTaskToStmgr), then do:
                    if (newSourceStmgrId != null && newDestionationStmgrId != null) {
                        LOG.info("1-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "] both has already assigned in NewTaskToStmagr map. No need to assinged.");
                    } else if (newSourceStmgrId != null && newDestionationStmgrId == null) {
                        LOG.info("2-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "], SourceTask has already assigned in NewTaskToStmagr map. Need to assgined destination task :" + destinationTaskId + "");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newSourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newSourceStmgrId).add(sourceTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newSourceStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newSourceStmgrId, newTaskToStmgr);
                            if (newSourceStmgrAvaliableTaskNum > 0) {
                                LOG.info("NEW. NewSourceStmgr still has avilable task num: " + newSourceStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + newSourceStmgrId);
                                newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            } else {
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                LOG.info("NEW. NewSrouceStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------
//                            newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);

                            // remove this pari from hotlist
                            LOG.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            LOG.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newSourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newSourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newSourceStmgrId).add(sourceTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newSourceStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newSourceStmgrId, newTaskToStmgr);
                            if (newSourceStmgrAvaliableTaskNum > 0) {
                                LOG.info("NEW. NewSourceStmgr still has avilable task num: " + newSourceStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + newSourceStmgrId);
                                newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            } else {
//                                LOG.info("NEW. NewSrouceStmgr do not have aviable task num. So the destination task: " +destinationTaskId+" will assigned to this original itself stmgr.");
//                                newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                // 2018-07-25 modified
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                LOG.info("NEW. NewSrouceStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------
//                            newTaskToStmgr.get(newSourceStmgrId).add(destinationTaskId);
                            // remove this pari from hotlist
                            LOG.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            LOG.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newSourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        }

                    } else if (newSourceStmgrId == null && newDestionationStmgrId != null) {
                        LOG.info("3-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "], DestinationTask has already assigned in NewTaskToStmagr map. Need to assgined destination task :" + sourceTaskId + "");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newDestionationStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(newDestionationStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newDestinationStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newDestionationStmgrId, newTaskToStmgr);
                            if (newDestinationStmgrAvaliableTaskNum > 0) {
                                LOG.info("NEW. NewDestionationStmgr still has avilable task num: " + newDestinationStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                LOG.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + newDestionationStmgrId);
                                newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
                            } else {
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                LOG.info("NEW. NewDestionationStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------

                            // remove this pari from hotlist
                            LOG.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            LOG.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newDestionationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + newDestionationStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(newDestionationStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newDestinationStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(newDestionationStmgrId, newTaskToStmgr);
                            if (newDestinationStmgrAvaliableTaskNum > 0) {
                                LOG.info("NEW. NewDestionationStmgr still has avilable task num: " + newDestinationStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                LOG.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + newDestionationStmgrId);
                                newTaskToStmgr.get(newDestionationStmgrId).add(sourceTaskId);
                            } else {
//                                LOG.info("NEW. NewDestionationStmgr do not have aviable task num. So the source task: " +sourceTaskId+" will assigned to this original itself stmgr.");
//                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                LOG.info("NEW. NewDestionationStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }
                            }
                            // -----------------------------------------------------------------------------------

                            // remove this pari from hotlist
                            LOG.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            LOG.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, newDestionationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        }

                    } else if (newSourceStmgrId == null && newDestionationStmgrId == null) {
                        LOG.info("4-HotEdge: [Source task: " + sourceTaskId + " and Destination task: " + destinationTaskId + "] both has already not assigned in NewTaskToStmagr map. Both need to assinged.");

                        if (judgeTaskOnSameStmgr(pair, taskToStmgr)) { // judge source task and destination task in on the same container/host or not, if on the same node, do this:
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: YES, in the same container/host: " + sourceStmgrId);
                            // common process...
                            LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + sourceStmgrId);
                            // add to newtaskToStmgr: reassignment task
//                            newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
//                            newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);

                            // 2018-07-21 add for validate stmgr task number -------------------------------------
                            int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                            if (newCommonStmgrAvaliableTaskNum > 0) {
                                // source task
                                LOG.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                LOG.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);

                                // the new task
                                newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    LOG.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                    LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                    newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);
                                } else {
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    LOG.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    }
                                }

                            } else {
                                // source task
                                String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                LOG.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                if (minStmgr != null) {
                                    newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                }

                                // destination task
                                int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                if (currentMinStmgrAvaliableTaskNum > 0) {
                                    LOG.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                    LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                    newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                } else {
                                    minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    LOG.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    }
                                }
                            }
                            // -----------------------------------------------------------------------------------
                            // remove this pari from hotlist
                            LOG.info("Removed this hot pair from hotPairList...");
                            hotPairList.remove(pair);

                            LOG.info("Now, find extra hot edge for this pair to assignment...");
                            assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, sourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);

                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        } else {
                            // get stmgr id for this pair
                            LOG.info("First, Judging this hotedge whether in the same container or not: NO, in the different container/host: (" + sourceStmgrId + ", " + destinationStmgrId + ")");
                            // common process...
                            // get host load using new task to stmgr map
                            long sourceStmgrLoad = getLoadForStmgr(sourceStmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                            long destiantionStmgrLoad = getLoadForStmgr(destinationStmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                            if (sourceStmgrLoad < destiantionStmgrLoad) {
                                LOG.info("-SourceStmgrLoad < DestinationStmgrLocd, so assign this pair to sourceStrmgrId...");

                                LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + sourceStmgrId);
                                // add to newtaskToStmgr: reassignment task
//                                newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);
//                                newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);

                                // 2018-07-21 add for validate stmgr task number -------------------------------------
                                int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    // source task
                                    LOG.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                    LOG.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                    newTaskToStmgr.get(sourceStmgrId).add(sourceTaskId);

                                    // the new task
                                    newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(sourceStmgrId, newTaskToStmgr);
                                    if (newCommonStmgrAvaliableTaskNum > 0) {
                                        LOG.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + sourceStmgrId);
                                        newTaskToStmgr.get(sourceStmgrId).add(destinationTaskId);
                                    } else {
                                        String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        LOG.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }

                                } else {
                                    // source task
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    LOG.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                    }

                                    // destination task
                                    int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                    if (currentMinStmgrAvaliableTaskNum > 0) {
                                        LOG.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    } else {
                                        minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        LOG.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }
                                }
                                // -----------------------------------------------------------------------------------

                                // remove this pari from hotlist
                                LOG.info("Removed this hot pair from hotPairList...");
                                hotPairList.remove(pair);

                                LOG.info("Now, find extra hot edge for this pair to assignment...");
                                assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, sourceStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);
                            } else {
                                LOG.info("-SourceStmgrLoad >= DestinationStmgrLocd, so assign this pair to destinationStmgrId...");

                                LOG.info("Then, assignment this host pair: " + pair + " to new packingplan: " + destinationStmgrId);
                                // add to newtaskToStmgr: reassignment task
//                                newTaskToStmgr.get(destinationStmgrId).add(sourceTaskId);
//                                newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                // 2018-07-21 add for validate stmgr task number -------------------------------------

                                int newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(destinationStmgrId, newTaskToStmgr);
                                if (newCommonStmgrAvaliableTaskNum > 0) {
                                    // source task
                                    LOG.info("NEW. 1-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned source task to this stmgr.");
                                    LOG.info("NEW. Will assigned source task : " + sourceTaskId + " to the same source task stmgr: " + destinationStmgrId);
                                    newTaskToStmgr.get(destinationStmgrId).add(sourceTaskId);

                                    // the new task
                                    newCommonStmgrAvaliableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(destinationStmgrId, newTaskToStmgr);
                                    if (newCommonStmgrAvaliableTaskNum > 0) {
                                        LOG.info("NEW. 2-NewCommonStmgr still has avilable task num: " + newCommonStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + destinationStmgrId);
                                        newTaskToStmgr.get(destinationStmgrId).add(destinationTaskId);
                                    } else {
                                        String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        LOG.info("NEW. 2-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }

                                } else {
                                    // source task
                                    String minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                    LOG.info("NEW. 1-NewCommonStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the source task: " + sourceTaskId + " will assigned to this minstmgr.");
                                    if (minStmgr != null) {
                                        newTaskToStmgr.get(minStmgr).add(sourceTaskId);
                                    }

                                    // destination task
                                    int currentMinStmgrAvaliableTaskNum = getTaskNumInNewStmgr(minStmgr, newTaskToStmgr);
                                    if (currentMinStmgrAvaliableTaskNum > 0) {
                                        LOG.info("NEW. 2-MinStmgr still has avilable task num: " + currentMinStmgrAvaliableTaskNum + ", can assigned destination task to this stmgr.");
                                        LOG.info("NEW. Will assigned destination task : " + destinationTaskId + " to the same source task stmgr: " + minStmgr);
                                        newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                    } else {
                                        minStmgr = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                                        LOG.info("NEW. 2-MinStmgr do not have aviable task num, find the min task num of stmgr that is: " + minStmgr + ". So the destination task: " + destinationTaskId + " will assigned to this minstmgr.");
                                        if (minStmgr != null) {
                                            newTaskToStmgr.get(minStmgr).add(destinationTaskId);
                                        }
                                    }
                                }
                                // -----------------------------------------------------------------------------------

                                // remove this pari from hotlist
                                LOG.info("Removed this hot pair from hotPairList...");
                                hotPairList.remove(pair);

                                LOG.info("Now, find extra hot edge for this pair to assignment...");
                                assignmentExtraHotEdgeForPair(sourceTaskId, destinationTaskId, destinationStmgrId, taskToStmgr, hotPairList, newTaskToStmgr);
                            }
                            LOG.info("Finally, ending assignment for this hot pair:" + pair);
                        }
                    }
                } /* - end if this pair is hot edge*/
            } /* - end for pair list */

            LOG.info("-------------------CURRENT STMGR->LIST OF TASK -START-------------------");
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
            LOG.info("Current unassignment task list: " + Utils.collectionToString(unAssignmentTaskList));

            // based on origin allocation to assignment these task
//            LOG.info("Based on origin allocation to assignment these unassignment tasks...");
//            for (int taskId : unAssignmentTaskList) {
//                if (getStmgrForTask(taskId, taskToStmgr) != null) {
//                    String stmgrId = getStmgrForTask(taskId, taskToStmgr);
//                    newTaskToStmgr.get(stmgrId).add(taskId);
//                }
//            }
            // ****************************************************************************
            LOG.info("Based on host load to assigned these unassignment tasks... -undo now");
            Map<String, Long> stmgrLoadMap = new HashMap<>(); // stmgrId -> stmgr load
            for (String hostname : stmgrToHostMap.keySet()) {
                List<String> stmgrList = stmgrToHostMap.get(hostname);
//                long hostLoad = 0l;
                for (String stmgrId : stmgrList) { // only one stmgr in one hostname
                    long stmgrLoad = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
//                    hostLoad += stmgrLoad;
                    stmgrLoadMap.put(stmgrId, stmgrLoad);
                    LOG.info("StmgrId: " + stmgrId + " -> load: " + stmgrLoad);
                }
            }

            LOG.info("Before assigned the unassignment task, current newTaskStmger map is: ");
            outputCurrentStmgrMap(newTaskToStmgr);

            LOG.info("Based on host task number to assigned these unassignment tasks... -do now");
            for (String stmgrId : newTaskToStmgr.keySet()) {
                List<Integer> temp = new ArrayList<>();
                int avilableTaskNum = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr); // still avilable task num of this stmgr
                int unassignmentTasks = unAssignmentTaskList.size();
                LOG.info("AvilableTaskNum is: " + avilableTaskNum + ", unAassignment task num is: " + unassignmentTasks);
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

            LOG.info("Current newTaskStmger map is: ");
            outputCurrentStmgrMap(newTaskToStmgr);
            LOG.info("-------------------CURRENT STMGR->LIST OF TASK -END-------------------");

            // get new allocation
            LOG.info("THEN. Get host edge allocation using newTaskToStmgr...");
            Map<Integer, List<InstanceId>> hotEdgeAllocation = getHotEdgeAllocation(newTaskToStmgr, physicalPlan);
            Set<PackingPlan.ContainerPlan> newContainerPlans = new HashSet<>();

            LOG.info("Creating new containerPlan...");
            for (int containerId : hotEdgeAllocation.keySet()) {
                List<InstanceId> instanceIdList = hotEdgeAllocation.get(containerId);
                //
                Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
                // unused in this class
                ByteAmount containerRam = containerRamPadding;
                LOG.info("Getting instance resource from packing plan...");
                for (InstanceId instanceId : instanceIdList) {
                    Resource resource = getInstanceResourceFromPackingPlan(instanceId.getTaskId(), packingPlan);
                    if (resource != null) {
                        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
                    } else {
                        LOG.info("Getting instance resource error!!!");
                    }
                }

                PackingPlan.ContainerPlan newContainerPlan = null;
                LOG.info("Get container resource from packing plan...");
                Resource containerRequiredResource = getContainerResourceFromPackingPlan(containerId, packingPlan);
                if (containerRequiredResource != null) {
                    newContainerPlan = new PackingPlan.ContainerPlan(containerId, new HashSet<>(instancePlanMap.values()), containerRequiredResource);
                } else {
                    LOG.info("Getting container resource error!!!");
                }
                newContainerPlans.add(newContainerPlan);
            }

            LOG.info("Creating new hot packing plan...");
            PackingPlan hotPackingPlan = new PackingPlan(topology.getId(), newContainerPlans);
            LOG.info("Validate Packing plan...");
            validatePackingPlan(hotPackingPlan);

            LOG.info("Now, Created hotPackingPlan successed, hot packing plan info is: ");
            outputPackingInfoToScheduleLog(hotPackingPlan);

            LOG.info("Now, Update topology with new packing by updateTopologyManager...");
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(hotPackingPlan);

            // **************************NEED TO CUSTOM ALGORITHM END***************************

            Config newRuntime = Config.newBuilder()
                    .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
                    .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateManagerAdaptor)
                    .build();

            // Create a ISchedulerClient basing on the config
            ISchedulerClient schedulerClient = getSchedulerClient(newRuntime);

            // build updatetopologyrequest object to update topogolgy
            Scheduler.UpdateTopologyRequest updateTopologyRequest =
                    Scheduler.UpdateTopologyRequest.newBuilder()
                            .setCurrentPackingPlan(currentPackingPlan)
                            .setProposedPackingPlan(proposedPackingPlan)
                            .build();

            LOG.info("Now, Update topology using updateTopologyManager...");
            LOG.info("Sending Updating Topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            // Clean the connection when we are done.
            LOG.info("Schedule update topology successfully!!!");
            LOG.info("After trigger scheduler, the physical plan is:");
            getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName));

            LOG.info("-----------------HOTEDGE RESCHEDULER END-----------------");
        } finally {
            // close zookeeper client connnection
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }

    /***************************Based Weight Scheduling Algorithm Start**************************/
    /**
     * 2018-09-16 add
     * Based weight scheduling algorithm
     *
     * @param packingPlan original packingplan
     */
    public void basedWeightSchedule(PackingPlan packingPlan) throws SQLException {
        LOG.info("-----------------BASED WEIGHT RESCHEDULING ALGORITHM START-----------------");
        String stateMgrClass = Context.stateManagerClass(this.config); // get state manager instance
        IStateManager stateMgr = null;

        try {
            stateMgr = ReflectionUtils.newInstance(stateMgrClass);
            FileUtils.writeToFile(filename, "[CORE] - Create IStateManager object success...");
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        try {
            stateMgr.initialize(this.config);
            SchedulerStateManagerAdaptor stateManagerAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);

            // current packing algorithm
            LOG.info("[CORE] - Current packing algorithm is: " + Context.packingClass(this.config));
            // physcial plan
            LOG.info("[CORE] - Current physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(this.topologyName);
            getPhysicalPlanInfo(physicalPlan);
            // packing plan
            LOG.info("[CORE] - Current packing plan is:");
            outputPackingInfoToScheduleLog(packingPlan);

            // **************************NEED TO CUSTOM ALGORITHM START***************************
            LOG.info("*****************CREATE NEW PACKINGPLAN USING CUSTOM ALGORITHM*****************");
            // get executorpair list from DB, pair list have sorted by traffic desc
            List<ExecutorPair> pairList = DataManager.getInstance().getInterExecutorTrafficList(this.topology.getId());
            // get stmgr to host map: hostname -> list of stmgr
            Map<String, List<String>> stmgrToHostMap = getStmgrToHost(physicalPlan);
            // get taskId to stmgr map: stmgrId -> list of taskid
            Map<String, List<Integer>> taskToStmgr = getTasksToStmgr(physicalPlan);
            // get taskId -> load
            Map<Integer, Long> taskLoadMap = DataManager.getInstance().getTaskLoadList();
            // ##20181108
//            List<Instance> instanceLoadList = DataManager.getInstance().getInstanceListByDesc();

            // get all instances of this topology
            List<PhysicalPlans.Instance> instancesList = getInstancesList(physicalPlan);
            // get all heron instance (spout and bolt)
            List<Integer> spoutInstanceList = getSpoutInstances(topology, instancesList);
            LOG.info("[BEFORE] - SpoutInstanceList: " + Utils.collectionToString(spoutInstanceList));

            List<Integer> boltInstanceList = getBoltInstances(topology, instancesList);
            LOG.info("[BEFORE] - BoltInstanceList: " + Utils.collectionToString(boltInstanceList));

            List<Integer> allInstanceIdList = new ArrayList<>();
            allInstanceIdList.addAll(spoutInstanceList);
            allInstanceIdList.addAll(boltInstanceList);
            LOG.info("[BEFORE] - ALlInstanceList: " + Utils.collectionToString(allInstanceIdList));

            // new task assignment allocation
            LOG.info("[CURRENT] - Create newTaskToStmgr: stmgr->taskList to build new ContainerPlan...");
            Map<String, List<Integer>> newTaskToStmgr = new HashMap<>();
            String lastStmgrId = "";
            for (String stmgrId : taskToStmgr.keySet()) {
                LOG.info("[INITIALIZE] - Initialize the stmgrId is: " + stmgrId);
                if (newTaskToStmgr.get(stmgrId) == null) {
                    newTaskToStmgr.put(stmgrId, new ArrayList<Integer>());
                }
                lastStmgrId = stmgrId;
            }
            LOG.info("[INITIALIZE] - Initialize stmgrId: " + lastStmgrId + " with all instances: " + Utils.collectionToString(allInstanceIdList));
            newTaskToStmgr.get(lastStmgrId).addAll(allInstanceIdList); // first assigned all task to the last stmgr

            // Based on Weight Scheduling Algorithm
            LOG.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM START********************");
            // calculating cluster load W, and average W of all worker node
            Long allWeight = getWeightOfCluster(taskLoadMap);
            // get instance number and calculate the average cpu load
            Long averageWeight = allWeight / taskToStmgr.keySet().size(); // the average weight of each worker node(stmgr) - expected weight value
            LOG.info("Calculate Result: [AllWeight: " + allWeight + "], [AverageWeight: " + averageWeight + "]");

//            if (stmgrToHostMap.keySet().size() == 1) {
//                LOG.info("[ATTENTION] - There is only one worker node(stmgr) in this cluster, don't processe this BW-Algorithm!");
//                LOG.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM END********************");
//                return;
//            } else {
//                LOG.info("[ATTENTION] - There is " + stmgrToHostMap.keySet().size() +" worker node(stmgr) in this cluster, can process BW-Algorithm...");
//            }

            // foreach worknode in the cluster
            for (String hostname : stmgrToHostMap.keySet()) {
                List<String> stmgrList = stmgrToHostMap.get(hostname);
                for (String stmgrId : stmgrList) { // only one stmgr in one worker node
                    if (stmgrId.equals(lastStmgrId)) {
                        LOG.info("[BREAK] - Current stmgr is the last stmgr: " + stmgrId + ", so break this loop!");
                        break;
                    }

                    LOG.info("[CURRENT] - Assigning tasks to [hostname: " + hostname + ", stmgrId: " + stmgrId + "]");
                    // get current stmgr's cpu load
                    long stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                    LOG.info("[CURRENT] - This stmgr initialize load weight is: " + stmgrWeight); // stmgrWeight will changed
                    // get current last stmgr task list
                    List<Integer> lastStmgrTaskList = newTaskToStmgr.get(lastStmgrId);

                    // 20181015 add instances number check ------------------------------
                    int instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    LOG.info("[CURRENT] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] instances. ");
                    // ------------------------------------------------------------------

                    // random choose one bolt or one spout when assigned one stmgr start
                    boolean hasBolt = hasBoltInLastStmgr(boltInstanceList, lastStmgrTaskList); // has bolt: true, don't has bolt: false
                    Integer willAssignmentTaskId = null;

                    // ##20181108 find max load task in the current last stmgr (unassignment task list)
                    willAssignmentTaskId = getMaxWorkloadInstanceInLastStmgr(taskLoadMap, newTaskToStmgr.get(lastStmgrId));
                    LOG.info("[##] - Current max workload task in the last stmgr is: " + willAssignmentTaskId);
//                    if (!hasBolt) { // last stmgr do not has bolt
//                        for (Integer lastStmgrTaskId : lastStmgrTaskList) {
//                            if (spoutInstanceList.contains(lastStmgrTaskId)) {
//                                LOG.info(" - There is a spout in last stmgr, its taskid is: " + lastStmgrTaskId);
//                                willAssignmentTaskId = lastStmgrTaskId;
//                                break;
//                            }
//                        }
//                    } else { // last stmgr do has bolt
//                        for (Integer lastStmgrTaskId : lastStmgrTaskList) {
//                            if (boltInstanceList.contains(lastStmgrTaskId)) {
//                                LOG.info(" - There is a bolt in last stmgr, its taskId is: " + lastStmgrTaskId);
//                                willAssignmentTaskId = lastStmgrTaskId;
//                                break;
//                            }
//                        }
//                    }
                    // update some value
                    if (willAssignmentTaskId != null) {
                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        LOG.info("[CHECK] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        if (instanceNumberInStmgr >= 4) {
                            break;
                        }
                        // ------------------------------------------------------------------

                        LOG.info("[CURRENT] - WillAssignmentTaskId is: " + willAssignmentTaskId);
                        newTaskToStmgr.get(stmgrId).add(willAssignmentTaskId);
                        newTaskToStmgr.get(lastStmgrId).remove(willAssignmentTaskId);
                        LOG.info("[CURRENT] - Current NewTaskToStmgr is: ");
                        outputTaskToStmgr(newTaskToStmgr);

                        stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                        LOG.info("[CURRENT] - stmgrWeight is: " + stmgrWeight);
                    } else {
                        LOG.info("[ATTENTION] - WillAssignmentTaskId is null!!! There is no task to assignment!!!");
                    }

                    // if there is only one stmgr(worker node), this whlie loop will not processing, because the stmgrweight will always > the averageWeight
                    // but if change < to <=, the while loop should will be processing. try it.
                    // now, there is only stmgr, so not to test following codes in while loop
//                    while (stmgrWeight < averageWeight && instanceNumberInStmgr <= 4) { // the condition for ending this loop
                    while (instanceNumberInStmgr <= 4) { // the condition for ending this loop

                        LOG.info("[CURRENT] - This stmgr current load weight is: " + stmgrWeight + " and AverageWeight is: " + averageWeight); // stmgrWeight will changed

                        /*************CALCULATE EdgeWeightGain START**************/
                        LOG.info("[EWG] - Calculating EdgeWeightGain of this willAssignmentTaskId: " + willAssignmentTaskId + " 's connected tasks...");
                        // ### Modified by yitian 2018-10-19 ---------------------------
                        // create a new list, it includes all tasks in current stmgr, and get the task with min EWG value in this list, not only the task with min EWG in the task list of current task connected
                        List<Integer> allTaskOfCurrentStmgr = new ArrayList<>();
                        for (int taskId : newTaskToStmgr.get(stmgrId)) {
                            List<Integer> connectedTaskList = getConnectedTasksInLastStmgr(taskId, pairList, newTaskToStmgr.get(lastStmgrId));
                            LOG.info("[CONNECTED-TASK] - The WillAssignmentTaskId: " + taskId + "'s connectedTaskList is: " + Utils.collectionToString(connectedTaskList));
                            allTaskOfCurrentStmgr.addAll(connectedTaskList);
                        }
                        // get connection task list of this willAssignmentTask
//                        List<Integer> connectedTaskList = getConnectedTasksInLastStmgr(willAssignmentTaskId, pairList, newTaskToStmgr.get(lastStmgrId));
//                        LOG.info("The WillAssignmentTaskId: " + willAssignmentTaskId + "'s connectedTaskList is: " + Utils.collectionToString(connectedTaskList));
                        //
                        LOG.info("[CONNECTED-TASK] - The CurrentStmgr: " + stmgrId + "'s allTaskOfCurrentStmgr is: " + Utils.collectionToString(allTaskOfCurrentStmgr));
                        // ### Modified by yitian 2018-10-19 ---------------------------
                        // foreach connectedTaskId in connectedTaskList
                        int maxEWG = Integer.MIN_VALUE;
                        int showWillAssignmentTaskId = willAssignmentTaskId; // record the willAssignmentTaskId to show
                        for (Integer connectedTaskId : allTaskOfCurrentStmgr) { // ### modified connectedTaskList -> allTaskOfCurrentStmgr
                            // calculating the pair list in current stmgr and last stmgr
                            List<ExecutorPair> pairListInCurrentStmgr = getConnectedTaskInStmgr(connectedTaskId, pairList, newTaskToStmgr.get(stmgrId));
                            LOG.info("[CURRENT] - Connected task: " + connectedTaskId + " of this taskId in stmgr_(" + stmgrId + ") is: " + Utils.collectionToString(pairListInCurrentStmgr));
                            // then getConnectedTaskInLastStmgr
                            List<ExecutorPair> pairListInLastStmgr = getConnectedTaskInStmgr(connectedTaskId, pairList, newTaskToStmgr.get(lastStmgrId));
                            LOG.info("[CURRENT] - Connected task " + connectedTaskId + " of this taskId in lastStmgr_(" + lastStmgrId + ") is: " + Utils.collectionToString(pairListInLastStmgr));

                            // calculating Edge Weight Gain of each connectedTaskId
                            // get Max(Edge Weight Gain) task to assign to this current stmgrId
                            int edgeWeightGain = calculateEdgeWeightGain(pairListInCurrentStmgr, pairListInLastStmgr);// return int value
                            LOG.info("- Current task( " + showWillAssignmentTaskId + " ) has connected task: " + connectedTaskId + ", its EWG is: " + edgeWeightGain);
                            if (edgeWeightGain > maxEWG) { // get max EWG of connected task
                                maxEWG = edgeWeightGain;
                                willAssignmentTaskId = connectedTaskId; // update the value of willAssignmentTaskId for next while loop
                            }
                            LOG.info("- And maxEWG: " + maxEWG + ". Then current willAssignmentTask is: " + willAssignmentTaskId);
                        }

                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        LOG.info("[CHECK] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        if (instanceNumberInStmgr >= 4) {
                            break;
                        }
                        // ------------------------------------------------------------------
                        // assignment willAssignmentTaskId's connected tasks
                        // update some value
                        newTaskToStmgr.get(stmgrId).add(willAssignmentTaskId); // will change this willAssignmentTaskId
                        newTaskToStmgr.get(lastStmgrId).remove(willAssignmentTaskId);
                        LOG.info("[CURRENT] - Current NewTaskToStmgr is: ");
                        outputTaskToStmgr(newTaskToStmgr);
                        /*************CALCULATE EdgeWeightGain END**************/

                        // update stmgr weight
                        stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                        LOG.info("[CURRENT] - StmgrWeight is: " + stmgrWeight);

                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        LOG.info("[UPDATE] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        // ------------------------------------------------------------------

                    }
                }
            }
            LOG.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM END********************");

            // output the newTaskToStmgr
            LOG.info("[AFTER] - The newTaskToStmgr is: ");
            outputTaskToStmgr(newTaskToStmgr);

            // above content wiil be changed ---------------- 2018-09-17 before go to Tianshan
            // get new allocation
            LOG.info("[THEN] - Get tasks allocation using newTaskToStmgr...");
            Map<Integer, List<InstanceId>> newAllocation = getAllocationBasedAlgorithm(newTaskToStmgr, physicalPlan);
            Set<PackingPlan.ContainerPlan> newContainerPlans = new HashSet<>();

            LOG.info("[THEN] - Creating new containerPlan...");
            for (int containerId : newAllocation.keySet()) {
                List<InstanceId> instanceIdList = newAllocation.get(containerId);
                //
                Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
                // unused in this class
                ByteAmount containerRam = containerRamPadding;
                LOG.info("- Getting instance resource from packing plan...");
                for (InstanceId instanceId : instanceIdList) {
                    Resource resource = getInstanceResourceFromPackingPlan(instanceId.getTaskId(), packingPlan);
                    if (resource != null) {
                        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
                    } else {
                        LOG.info("Getting instance resource error!!!");
                    }
                }

                PackingPlan.ContainerPlan newContainerPlan = null;
                LOG.info("- Get container resource from packing plan...");
                Resource containerRequiredResource = getContainerResourceFromPackingPlan(containerId, packingPlan);
                if (containerRequiredResource != null) {
                    newContainerPlan = new PackingPlan.ContainerPlan(containerId, new HashSet<>(instancePlanMap.values()), containerRequiredResource);
                } else {
                    LOG.info("Getting container resource error!!!");
                }
                newContainerPlans.add(newContainerPlan);
            }

            LOG.info("[NOW] - Creating new packing plan...");
            PackingPlan basedWeightPackingPlan = new PackingPlan(topology.getId(), newContainerPlans);
            LOG.info("[NOW] - Validate Packing plan...");
            validatePackingPlan(basedWeightPackingPlan);

            LOG.info("=========================CREATED PACKING PLAN SUCCESSED=========================");
//            outputPackingInfoToScheduleLog(basedWeightPackingPlan);

            LOG.info("[NOW] - Update topology with new packing by updateTopologyManager...");
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(basedWeightPackingPlan);

            // **************************NEED TO CUSTOM ALGORITHM END***************************

            Config newRuntime = Config.newBuilder()
                    .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
                    .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateManagerAdaptor)
                    .build();

            // Create a ISchedulerClient basing on the config
            ISchedulerClient schedulerClient = getSchedulerClient(newRuntime);

            // build updatetopologyrequest object to update topogolgy
            Scheduler.UpdateTopologyRequest updateTopologyRequest =
                    Scheduler.UpdateTopologyRequest.newBuilder()
                            .setCurrentPackingPlan(currentPackingPlan)
                            .setProposedPackingPlan(proposedPackingPlan)
                            .build();

            LOG.info("[CORE] - Update topology using updateTopologyManager...");
            LOG.info("[CORE] - Sending updating topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "[CORE] - Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            // Clean the connection when we are done.
            LOG.info("[CORE] - =========================UPDATE TOPOLOGY SUCCESSFULLY=========================");
            LOG.info("[CORE] - After trigger scheduler, the physical plan is:");
            getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName));

            LOG.info("-----------------BASED WEIGHT RESCHEDULING ALGORITHM END-----------------");
        } finally {
            // close zookeeper client connnection
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }

    /***************************Based Weight Scheduing Algorithm End**************************/

    /**
     * Test schedule function
     * build time: 2018/09/20
     *
     * @param packingPlan
     */
    public void testSchedule(PackingPlan packingPlan) {
        LOG.info("=======================THIS IS TEST SCHEDULE START=======================");
        String stateMgrClass = Context.stateManagerClass(this.config);
        IStateManager stateManager = null;
        AuroraCustomScheduler customScheduler = null;
        LOG.info("StateMgrClass is: " + stateMgrClass);
        LOG.info("TopologyName is: " + topologyName);

        try {
            stateManager = ReflectionUtils.newInstance(stateMgrClass);
            LOG.info("Statemgr created success !!!");
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        try {
            stateManager.initialize(this.config);
            SchedulerStateManagerAdaptor stateManagerAdaptor = new SchedulerStateManagerAdaptor(stateManager, 5000);
            LOG.info("Current packing algorithm is: " + Context.packingClass(this.config));

            // this collected
            PackingPlans.PackingPlan protoPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlan deserializedPackingPlan = deserializer.fromProto(protoPackingPlan);
            outputPackingInfoToScheduleLog(deserializedPackingPlan);

            // something wrong
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(this.topologyName);
            getPhysicalPlanInfo(physicalPlan);


            LOG.info("=======================THIS IS TEST SCHEDULE END=======================");
        } finally {
            SysUtils.closeIgnoringExceptions(stateManager);
        }

    }

    /*******************************************************************************/
    /*                 Based Weight Scheduling Algorithm Tools Start               */

    /*******************************************************************************/
    // get weight of all cluster
    private Long getWeightOfCluster(Map<Integer, Long> taskLoadMap) {
        Long weight = 0l;
        for (Integer taskId : taskLoadMap.keySet()) {
            weight += taskLoadMap.get(taskId);
        }
        return weight;
    }

    private List<Integer> getSpoutInstances(TopologyAPI.Topology topology, List<PhysicalPlans.Instance> instancesList) {
        List<Integer> spoutInstances = new ArrayList<>();

        List<TopologyAPI.Spout> spoutList = topology.getSpoutsList();
        List<String> spoutNameList = new ArrayList<>();
        for (TopologyAPI.Spout spout : spoutList) {
            spoutNameList.add(spout.getComp().getName());
        }

        for (PhysicalPlans.Instance instance : instancesList) {
            String instanceComponentName = instance.getInfo().getComponentName();
            if (spoutNameList.contains(instanceComponentName)) {
                spoutInstances.add(instance.getInfo().getTaskId());
            }
        }
        return spoutInstances;
    }

    private List<Integer> getBoltInstances(TopologyAPI.Topology topology, List<PhysicalPlans.Instance> instancesList) {
        List<Integer> boltInstances = new ArrayList<>();

        List<TopologyAPI.Bolt> boltList = topology.getBoltsList();
        List<String> boltNameList = new ArrayList<>();
        for (TopologyAPI.Bolt bolt : boltList) {
            boltNameList.add(bolt.getComp().getName());
        }

        for (PhysicalPlans.Instance instance : instancesList) {
            String instanceComponentName = instance.getInfo().getComponentName();
            if (boltNameList.contains(instanceComponentName)) {
                boltInstances.add(instance.getInfo().getTaskId());
            }
        }
        return boltInstances;
    }

    /**
     * search the connected task list of current willAssignmentTask in the last stmgr(worker node:logistic)
     *
     * @param taskId            Example:B
     * @param pairList
     * @param lastStmgrTaskList
     * @return the willAssignmentTask's connected task list in the last stmgr (worker node)
     */
    private List<Integer> getConnectedTasksInLastStmgr(int taskId, List<ExecutorPair> pairList, List<Integer> lastStmgrTaskList) {
        List<Integer> connectedTaskList = new ArrayList<>();
        for (ExecutorPair pair : pairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinatioinTaskId = pair.getDestination().getBeginTask();

            if (sourceTaskId == taskId) { // B -> C
                connectedTaskList.add(destinatioinTaskId);
            }
            if (destinatioinTaskId == taskId) { // A -> B
                connectedTaskList.add(sourceTaskId); //
            }
        }

        List<Integer> connectedTaskListInLastStmgr = new ArrayList<>();
        for (int connectedTaskId : connectedTaskList) {
            if (lastStmgrTaskList.contains(connectedTaskId)) {
                connectedTaskListInLastStmgr.add(connectedTaskId);
            }
        }
        return connectedTaskListInLastStmgr;
    }

    private boolean hasBoltInLastStmgr(List<Integer> boltInstanceList, List<Integer> lastStmgrTaskList) {
        boolean hasBolt = false;

        for (Integer lastStmgrTaskId : lastStmgrTaskList) {
            if (boltInstanceList.contains(lastStmgrTaskId)) {
                hasBolt = true;
                break;
            }
        }
        return hasBolt;
    }

    /**
     * search connected task list of a task in current stmgr (not last stmgr)
     *
     * @param taskId
     * @param pairList
     * @param stmgrTaskList
     * @return connected executor pair
     */
    private List<ExecutorPair> getConnectedTaskInStmgr(int taskId, List<ExecutorPair> pairList, List<Integer> stmgrTaskList) {
        List<ExecutorPair> connectedPair = new ArrayList<>();
        List<ExecutorPair> connectedPairInCurrentStmgr = new ArrayList<>();
        // ###20181108 add
        List<Integer> stmgrTaskListTemp = UtilFunctions.deepCopyIntegerArrayList(stmgrTaskList);

        // seaching connected task of the taskId
        for (ExecutorPair pair : pairList) { // foreach all instance pair
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if (taskId == sourceTaskId || taskId == destinationTaskId) {
                connectedPair.add(pair);
            }
        }
        // ###20181108 add
        Integer taskIdObject = new Integer(taskId);
        stmgrTaskListTemp.remove(taskIdObject);
//        LOG.info("[FUNCTION] - stmgrTaskListTemp: " + Utils.collectionToString(stmgrTaskListTemp));

        // and searching connected tasks in this stmgr
        for (ExecutorPair pair : connectedPair) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if (stmgrTaskListTemp.contains(sourceTaskId) || stmgrTaskListTemp.contains(destinationTaskId)) {
                connectedPairInCurrentStmgr.add(pair);
            }
        }
        return connectedPairInCurrentStmgr;
    }

    /**
     * calculating the Edge Weight Gain of this connected task
     *
     * @param pairListInCurrentStmgr
     * @param pairListInLastStmgr
     * @return
     */
    private int calculateEdgeWeightGain(List<ExecutorPair> pairListInCurrentStmgr, List<ExecutorPair> pairListInLastStmgr) {
        int taskEWG = 0;
        int stmgrEWG = 0;
        int lastEWG = 0;

        // remove repeat ExecutorPair in last stmgr list
        for (ExecutorPair pair : pairListInCurrentStmgr) {
            if (pairListInLastStmgr.contains(pair)) {
                pairListInLastStmgr.remove(pair);
            }
        }

        // calculate Edge Weight Gain value
        for (ExecutorPair pair : pairListInCurrentStmgr) {
            stmgrEWG += pair.getTraffic();
        }
        for (ExecutorPair pair : pairListInLastStmgr) {
            lastEWG += pair.getTraffic();
        }
//        taskEWG = lastEWG - stmgrEWG; // scheduling task from n to cuurent stmgr
        taskEWG = stmgrEWG - lastEWG;
        return taskEWG;
    }

    /**
     * @param taskToStmgr
     * @param physicalPlan
     * @return
     */
    private Map<Integer, List<InstanceId>> getAllocationBasedAlgorithm(Map<String, List<Integer>> taskToStmgr, PhysicalPlans.PhysicalPlan physicalPlan) {
        LOG.info("[FUNCTION]----------------CREATE ALLOCATION BASED ON CUSTOM ALGORITHM START----------------");
        Map<Integer, List<InstanceId>> allocation = new HashMap<>();
        int numContainer = TopologyUtils.getNumContainers(topology);
        int totalInstance = TopologyUtils.getTotalInstance(topology);
        List<PhysicalPlans.Instance> instanceList = getInstancesList(physicalPlan);

        if (numContainer > totalInstance) {
            throw new RuntimeException("- More containers allocated than instances.");
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
            LOG.info("- Container: " + containerId + " has new allocation: " + Utils.collectionToString(allocation.get(containerId)));
        }
        LOG.info("[FUNCTION]----------------CREATE ALLOCATION BASED ON CUSTOM ALGORITHM END----------------");
        return allocation;
    }

    /**
     * output the taskToStmgr map info
     *
     * @param newTaskToStmgr
     */
    private void outputTaskToStmgr(Map<String, List<Integer>> newTaskToStmgr) {
        LOG.info("[FUNCTION]------------OUTPUT STMGR->TASKS START------------");
        for (String stmgrId : newTaskToStmgr.keySet()) {
            LOG.info("NewTaskToStmgr: " + stmgrId + " has tasks: " + Utils.collectionToString(newTaskToStmgr.get(stmgrId)));
        }
        LOG.info("[FUNCTION]------------OUTPUT STMGR->TASKS END------------");
    }

    /**
     * @param willAssignmentTaskId
     * @param connectedTaskId
     * @param pairList
     */
    private void updateExecutorPairList(Integer willAssignmentTaskId, Integer connectedTaskId, List<ExecutorPair> pairList) {
        LOG.info("-------------------------UPDATE EXECUTOR PAIR LIST START-------------------------");
        LOG.info("[FUNCTION] - Update Executor Pair List with: [" + willAssignmentTaskId + ", " + connectedTaskId + "]");
        for (ExecutorPair pair : pairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if ((sourceTaskId == willAssignmentTaskId && destinationTaskId == connectedTaskId) || (sourceTaskId == connectedTaskId && destinationTaskId == willAssignmentTaskId)) {
                LOG.info("- Will remove pair from ExecutorPairList is: " + pair);
                pairList.remove(pair);
            }
        }
        LOG.info("-------------------------UPDATE EXECUTOR PAIR LIST END-------------------------");
    }

    /**
     * ##20181108 getMaxWorkloadInstanceInLastStmgr
     * @param taskLoadMap
     * @param lastStmgrTaskList
     * @return taskIdOfMaxLoad
     */
    private Integer getMaxWorkloadInstanceInLastStmgr(Map<Integer, Long> taskLoadMap, List<Integer> lastStmgrTaskList) {
        long maxWorkLoad = Long.MIN_VALUE;
        int taskIdOfMaxLoad = 1;

        LOG.info("[FUNCTION] - Current task list in last stmgr is: " + Utils.collectionToString(lastStmgrTaskList));
        for (int taskId : lastStmgrTaskList) {
            long workload = taskLoadMap.get(taskId);
            LOG.info("[FUNCTION] - Current taskId: " + taskId + ", workload: " + workload);
            if (workload > maxWorkLoad) {
                maxWorkLoad = workload;
                taskIdOfMaxLoad = taskId;
            }
        }
        return taskIdOfMaxLoad;
    }
    /*******************************************************************************/
    /*                   Based Weight Scheduling Algorithm Tools End               */
    /*******************************************************************************/


    /*********************************RESCOURCE ABOUT START*********************************/
//    private ByteAmount getContainerRamPadding(List<TopologyAPI.Config.KeyValue> topologyConfig) {
//        return TopologyUtils.getConfigWithDefault(topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_RAM_PADDING, DEFAULT_RAM_PADDING_PER_CONTAINER);
//    }
    private Resource getInstanceResourceFromPackingPlan(int taskId, PackingPlan packingPlan) {
        Resource resource = null;
        Map<Integer, PackingPlan.ContainerPlan> containerPlanMap = packingPlan.getContainersMap();
        for (Integer containerId : containerPlanMap.keySet()) {
            PackingPlan.ContainerPlan containerPlan = containerPlanMap.get(containerId);
            Set<PackingPlan.InstancePlan> instancePlanSet = containerPlan.getInstances();
            for (PackingPlan.InstancePlan instancePlan : instancePlanSet) {
                if (instancePlan.getTaskId() == taskId) {
                    resource = instancePlan.getResource();
                }
            }
        }
        return resource;
    }

    private Resource getContainerResourceFromPackingPlan(int containerId, PackingPlan packingPlan) {
        Resource resource = null;
        Map<Integer, PackingPlan.ContainerPlan> containerPlanMap = packingPlan.getContainersMap();
        for (Integer id : containerPlanMap.keySet()) {
            PackingPlan.ContainerPlan containerPlan = containerPlanMap.get(id);
            if (id == containerId) {
                resource = containerPlan.getRequiredResource();
            }
        }
        return resource;
    }

    /**
     * ****************************************
     * add: 2081-05-21 from packing algorithem
     * ****************************************
     * Check whether the PackingPlan generated is valid
     * 这里只是对最后生成的InstanceRam资源是否小于MIN_RAM_PER_INSTANCE进行判断
     *
     * @param plan
     */
    private void validatePackingPlan(PackingPlan plan) throws PackingException {
        for (PackingPlan.ContainerPlan containerPlan : plan.getContainers()) {
            for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
                // safe check
                if (instancePlan.getResource().getRam().lessThan(MIN_RAM_PER_INSTANCE)) {
                    throw new PackingException(String.format("Invalid packing plan generated. A minimum of "
                                    + "%s ram is required, but InstancePlan for component '%s' has %s",
                            MIN_RAM_PER_INSTANCE, instancePlan.getComponentName(),
                            instancePlan.getResource().getRam()));
                }
            }
        }
    }
    /*********************************RESCOURCE ABOUT END*********************************/


    /****************************************************************************/
    //                  Hot Edge Reschedule Tools function Start
    /****************************************************************************/

    /**
     * @param physicalPlan
     * @return
     */
    private Map<String, List<String>> getStmgrToHost(PhysicalPlans.PhysicalPlan physicalPlan) {
        LOG.info("[FUNCTION]--------------GET HOSTNAME -> STMGRS LIST START--------------");
        LOG.info("Getting hostname from physical plan...");
        List<String> hostList = new ArrayList<>(); // hostname list in this topology
        for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) {
            String hostname = stMgr.getHostName();
            if (!hostList.contains(hostname)) {
                hostList.add(hostname);
            }
        }
        LOG.info("Hostlist: " + Utils.collectionToString(hostList));
        // hostname -> list of stmgrid
        LOG.info("Getting host -> stmgr list from physical plan...");
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
            LOG.info("hostname: " + hostname + " stmgr list: " + Utils.collectionToString(stmgrToHostMap.get(hostname)));
        }
        LOG.info("[FUNCTION]--------------GET HOSTNAME -> STMGRS LIST END--------------");
        return stmgrToHostMap;
    }

    /**
     * @param physicalPlan
     * @return
     */
    private Map<String, List<Integer>> getTasksToStmgr(PhysicalPlans.PhysicalPlan physicalPlan) {
        LOG.info("[FUNCTION]------------GET STMGR -> TASKS LIST START------------");
        LOG.info("Getting stmgr -> task list from physical plan...");
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
            LOG.info("Stmgr id: " + stMgr.getId() + " instance list is: " + taskToContainerMap.get(stMgr.getId()));
        }
        LOG.info("[FUNCTION]------------GET STMGR -> TASKS LIST END------------");
        return taskToContainerMap;
    }

    private boolean judgeTaskOnSameStmgr(ExecutorPair pair, Map<String, List<Integer>> taskToStmgr) {
        LOG.info("Invoking...Judging task on the same container or not...");
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

    /**
     * @param taskId
     * @param taskToStmgr
     * @return
     */
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

    /**
     * @param sourceHotTaskId
     * @param hotPairList
     * @return
     */
    private List<ExecutorPair> getExtraHotEdgeForSourceTask(int sourceHotTaskId, List<ExecutorPair> hotPairList) {
        List<ExecutorPair> extraHotPairList = new ArrayList<>();
        for (ExecutorPair pair : hotPairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if (sourceHotTaskId == destinationTaskId) { // m -> i is hot edge
                extraHotPairList.add(pair);
                LOG.info("Source taskid: [" + sourceHotTaskId + "], has extra hot edge: " + pair);
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
                LOG.info("Destination taskid: [" + destinationTaskId + "], has extra hot edge: " + pair);
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
        LOG.info("[FUNCTION]------------GET STMGR CURRENT LOAD START------------");
        long stmgrLoad = 0l;
        String currentHostname = "";
        LOG.info("Get hostname of this stmger: " + stmgrId);
        for (String hostname : stmgrToHostMap.keySet()) {
            List<String> stmgrList = stmgrToHostMap.get(hostname);
            for (String stmgrItem : stmgrList) {
                if (stmgrId.equals(stmgrItem)) {
                    currentHostname = hostname;
                    break;
                } else {
                    LOG.info("Not find the stgmrID: " + stmgrId);
                }
            }
        }
        LOG.info("Calculate stmgr load of current host: " + currentHostname);
        List<Integer> taskList = newTaskToStmgr.get(stmgrId);
        for (Integer taskId : taskList) {
            long taskLoad = taskLoadMap.get(taskId);
            LOG.info(taskId + " 's load is: " + taskLoad);
            stmgrLoad += taskLoad;
        }
        LOG.info("[FUNCTION]------------GET STMGR CURRENT LOAD END------------");
        return stmgrLoad;
    }

    /**
     * @param taskToStmgr
     * @return
     */
    private Map<Integer, List<InstanceId>> getHotEdgeAllocation(Map<String, List<Integer>> taskToStmgr, PhysicalPlans.PhysicalPlan physicalPlan) {
        LOG.info("[FUNCTION]----------CREAT NEW HOT ALLOCATION USING TASKTOSTMGR START----------");
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
            LOG.info("Container: " + containerId + " has new allocation: " + Utils.collectionToString(allocation.get(containerId)));
        }
        LOG.info("[FUNCTION]----------CREAT NEW HOT ALLOCATION USING TASKTOSTMGR END----------");
        return allocation;
    }

    /**
     * @param physicalPlan
     * @return
     */
    private List<PhysicalPlans.Instance> getInstancesList(PhysicalPlans.PhysicalPlan physicalPlan) {
        LOG.info("Getting instance list from physical plan...");
        List<PhysicalPlans.Instance> instanceList = physicalPlan.getInstancesList();
        return instanceList;
    }

    /**
     * @param taskId
     * @param instanceList
     * @return
     */
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
        LOG.info("------------------ASSIGNMENT EXTRA HOT EDGE FOR PAIR -START------------------");

        // find extra hot edge in hot pari list of this task
        LOG.info("Getting extra task for this source task in this hot edge...");
        List<ExecutorPair> extraSourceHotPairList = getExtraHotEdgeForSourceTask(sourceTaskId, hotPairList);
        for (ExecutorPair extraPair : extraSourceHotPairList) {
            int extraHotSourceTaskId = extraPair.getSource().getBeginTask();

            // ## yitian added for ADT
            String hasAssignedForExtraHotSourceTask = getStmgrForTask(extraHotSourceTaskId, newTaskToStmgr);
            if (hasAssignedForExtraHotSourceTask == null) { // this extra hot source task has not assigned
                // move extra taskid to current source task stmgr
                String extraStmgrId = getStmgrForTask(extraHotSourceTaskId, taskToStmgr); // extra task stmgrid
                LOG.info("The extra task of source task is: " + extraHotSourceTaskId + ", it's stmgrId: " + extraStmgrId);

                if (judgeTaskOnSameStmgr(extraPair, taskToStmgr)) { // if extra task is stll on the same stmgr with current taskid
                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        LOG.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraHotSourceTaskId);
                        LOG.info("The extra hot task: " + extraHotSourceTaskId + " is still on the same stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
                        LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            LOG.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraHotSourceTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------
                } else {
                    LOG.info("Current assignment of this extra pair is: ( " + sourceTaskId + ": " + stmgrId + ", " + extraHotSourceTaskId + ": " + extraStmgrId + " ) ");
                    LOG.info("These task on different container... We should reassignment this extra task to: " + stmgrId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        LOG.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraHotSourceTaskId);
                        LOG.info("The extra hot task: " + extraHotSourceTaskId + " has assigned to stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
//                    LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned original itself stmgr.");
//                    newTaskToStmgr.get(extraStmgrId).add(extraHotSourceTaskId);
                        LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            LOG.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraHotSourceTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------
                }
            } else {
                LOG.info("The extra source task has assigned in newTaskToStmgr: " + hasAssignedForExtraHotSourceTask);
            }


            // 2018-07-27 add for bug ---------------------------------------
            // remove this pari from hotlist
            LOG.info("Removed this hot pair from hotPairList...");
            hotPairList.remove(extraPair);
            // --------------------------------------------------------------
        }

        LOG.info("Getting extra task for this destination task in this hot edge..");
        List<ExecutorPair> extraDestinationHotPairList = getExtraHotEdgeForDestinationTask(destinationTaskId, hotPairList);
        for (ExecutorPair extraPair : extraDestinationHotPairList) {
            int extraDestinationTaskId = extraPair.getDestination().getBeginTask();

            // ## yitian added for ADT
            String hasAssignedForExtraHotDestinationTask = getStmgrForTask(extraDestinationTaskId, newTaskToStmgr);
            if (hasAssignedForExtraHotDestinationTask == null) { // this extra hot source task has not assigned
                String extraStmgrId = getStmgrForTask(extraDestinationTaskId, taskToStmgr);
                LOG.info("The extra task of destination task is: " + extraDestinationTaskId + ", It's stmgrId: " + extraStmgrId);

                if (judgeTaskOnSameStmgr(extraPair, taskToStmgr)) {
//                    newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
//                    LOG.info("The extra hot task: " + extraDestinationTaskId + " is still on the same stmgr with current destination task: " + destinationTaskId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        LOG.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
                        LOG.info("The extra hot task: " + extraDestinationTaskId + " is still on the same stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
                        LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            LOG.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraDestinationTaskId);
                        }
                    }
                    // ------------------------------------------------------------------------------

                } else {
                    LOG.info("Current assignment of this extra pair is: ( " + destinationTaskId + ": " + stmgrId + ", " + extraDestinationTaskId + ": " + extraStmgrId + " ) ");
                    LOG.info("These task on different container... We should reassignment this extra task to: " + stmgrId);

                    // 20180721-add for validate task num of one stmgr ------------------------------
                    int availableCount = TASK_NUM_PER_CONTAINER - getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    if (availableCount > 0) { // can assgined
                        LOG.info("Target stmgr: " + stmgrId + " still avilable task num is : " + availableCount + ". So this task will assigned this stmgr...");
                        newTaskToStmgr.get(stmgrId).add(extraDestinationTaskId);
                        LOG.info("The extra hot task: " + extraDestinationTaskId + " has assigned to stmgr: " + stmgrId + " with current source task: " + sourceTaskId);
                    } else { // cannot assgined this task,how to deal with it?
//                    LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned original itself stmgr.");
//                    newTaskToStmgr.get(extraStmgrId).add(extraDestinationTaskId);
                        LOG.info("Target stmgr: " + stmgrId + " cannot avilable any task!. So this task will assigned anothor stmgr...");
                        String minStmger = getMinTaskNumOfNewStmgr(newTaskToStmgr);
                        if (minStmger != null) {
                            LOG.info("Find min task number of stmgr: " + minStmger + ". This task will assigned to this min stmgr.");
                            newTaskToStmgr.get(minStmger).add(extraDestinationTaskId);
                        }
                    }
                    // -----------------------------------------------------------------------------
                }
            } else {
                LOG.info("The extra destination task has assigned in newTaskToStmgr: " + hasAssignedForExtraHotDestinationTask);
            }

            // 2018-07-27 add for bug ---------------------------------------
            // remove this pari from hotlist
            LOG.info("Removed this hot pair from hotPairList...");
            hotPairList.remove(extraPair);
            // --------------------------------------------------------------
        }
        LOG.info("------------------ASSIGNMENT EXTRA HOT EDGE FOR PAIR -END------------------");
    }

    /**
     * @param stmgrId
     * @param newTaskToStmgr
     * @return
     */
    private int getTaskNumInNewStmgr(String stmgrId, Map<String, List<Integer>> newTaskToStmgr) {
        int resultCount = 0;
        List<Integer> taskListOfStmgr = newTaskToStmgr.get(stmgrId);
        if (taskListOfStmgr.size() != 0) {
            resultCount = taskListOfStmgr.size();
        }
        return resultCount;
    }

    /**
     * @param newTaskToStmgr
     * @return
     */
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

    /**
     * @param newTaskToStmgr
     */
    private void outputCurrentStmgrMap(Map<String, List<Integer>> newTaskToStmgr) {
        LOG.info("---------------CURRENT NEW STMGR->TASK MAP START---------------");
        for (String stmgrId : newTaskToStmgr.keySet()) {
            List<Integer> taskList = newTaskToStmgr.get(stmgrId);
            LOG.info("StmgrId: " + stmgrId + " -> " + Utils.collectionToString(taskList));
        }
        LOG.info("---------------CURRENT NEW STMGR->TASK MAP END---------------");

    }
    /****************************************************************************/
    //                  Hot Edge Reschedule Tools function End
    /****************************************************************************/


    /****************************************************************************/
    //                       Start ReScheduler Tools Functions
    /****************************************************************************/

    /**
     * Get current physical plan info to reschedule topology
     * add: 2018-05-19
     *
     * @return
     */
    public PhysicalPlans.PhysicalPlan getPhysicalPlanInfo(PhysicalPlans.PhysicalPlan physicalPlan) {
        FileUtils.writeToFile(filename, "[FUNCTION]----------------GET PHYSICAL PLAN START----------------");
        FileUtils.writeToFile(filename, "Getting hosts list of word nodes from physical plan...");
        List<String> hostList = new ArrayList<>(); // hostname list in this topology
        for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) { // get hostanem from Stmgr class
            String hostname = stMgr.getHostName();
            if (!hostList.contains(hostname)) {
                hostList.add(hostname);
            }
        }
        FileUtils.writeToFile(filename, "Hostlist: " + zyt.custom.my.scheduler.Utils.collectionToString(hostList));

        // hostname -> list of stmgrid
        FileUtils.writeToFile(filename, "Getting host -> stmgr list from physical plan...");
        Map<String, List<String>> stmgrToHostMap = new HashMap<>();
        for (String hostname : hostList) {
            List<String> stmgrList = null;
            if (stmgrToHostMap.get(hostname) == null) {
                stmgrList = new ArrayList<>();
                stmgrToHostMap.put(hostname, stmgrList);
            }
            for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) {
                if (stMgr.getHostName().equals(hostname)) {
                    stmgrList.add(stMgr.getId());
                }
            }
            stmgrToHostMap.put(hostname, stmgrList);
            FileUtils.writeToFile(filename, "Hostname: " + hostname + " stmgr list: " + zyt.custom.my.scheduler.Utils.collectionToString(stmgrToHostMap.get(hostname)));
        }

        FileUtils.writeToFile(filename, "Getting stmgr -> task list from physical plan...");
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
            FileUtils.writeToFile(filename, "Stmgr id: " + stMgr.getId() + " instance list is: " + taskToContainerMap.get(stMgr.getId()));
        }

        FileUtils.writeToFile(filename, "[FUNCTION]----------------GET PHYSICAL PLAN END----------------");
        return physicalPlan;
    }

    /**
     * Output config and runtime info to log file
     *
     * @param config
     * @param runtime
     */
    private void outputConfigInfo(Config config, Config runtime) {
        FileUtils.writeToFile(filename, "--------------------CONFIG INFO START----------------------");
        FileUtils.writeToFile(filename, config.toString());
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO START---------------------");
        FileUtils.writeToFile(filename, runtime.toString());
        FileUtils.writeToFile(filename, "--------------------CONFIG AND RUNTIME INFO END--------------------------");
    }

    private void outputRuntimeInfo(Config config) {
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO START----------------------");
        FileUtils.writeToFile(filename, config.toString());
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO END--------------------------");
    }

    /**
     * @param packingPlan
     */
    private void outputPackingInfoToScheduleLog(PackingPlan packingPlan) {
        FileUtils.writeToFile(filename, "[FUNCTION]---------------PACKING PLAN INFO START---------------");
        FileUtils.writeToFile(filename, "PackingPlan info: " + packingPlan);
        Map<Integer, PackingPlan.ContainerPlan> containersMap = packingPlan.getContainersMap();
        for (Integer integer : containersMap.keySet()) {
            PackingPlan.ContainerPlan containerPlan = containersMap.get(integer);
            FileUtils.writeToFile(filename, "ContainerPlan info: " + containerPlan); // print current containerPlan
            Set<PackingPlan.InstancePlan> instancePlanSet = containerPlan.getInstances();
            for (PackingPlan.InstancePlan instancePlan : instancePlanSet) {
                FileUtils.writeToFile(filename, "InstancePlan info: " + instancePlan); // 使用tostring方法输出instancePlan
            }
        }
        FileUtils.writeToFile(filename, "[FUNCTION]---------------PACKING PLAN INFO END---------------");
    }

    /**
     *
     */
    private void getTopologyInfo() {
        // modified 2018-05-11 to show stream info in topology
        FileUtils.writeToFile(filename, "-------------------------TOPOLOGY INFO START-------------------------");
        // 输出Topology信息
        TopologyAPI.Topology topology = Runtime.topology(runtime);
        FileUtils.writeToFile(filename, "Topology id: " + topology.getId() + " name: " + topology.getName());
        FileUtils.writeToFile(filename, "Spout info...");
        List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
        for (TopologyAPI.Spout spout : spouts) {
            FileUtils.writeToFile(filename, "Spout comp name: " + spout.getComp().getName());
            FileUtils.writeToFile(filename, "Spout outputs count: " + spout.getOutputsCount());
            FileUtils.writeToFile(filename, "Get spout output list...");
            List<TopologyAPI.OutputStream> outputStreams = spout.getOutputsList();
            for (TopologyAPI.OutputStream outputStream : outputStreams) {
                TopologyAPI.StreamId streamId = outputStream.getStream();
                FileUtils.writeToFile(filename, "Spout output stream: " + streamId);
                FileUtils.writeToFile(filename, "Spout Output Stream id: " + streamId.getId());
                FileUtils.writeToFile(filename, "This Stream's component name: " + streamId.getComponentName());
            }
        }
        FileUtils.writeToFile(filename, "Bolt info...");
        List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
        for (TopologyAPI.Bolt bolt : bolts) {
            FileUtils.writeToFile(filename, "Bolt comp name: " + bolt.getComp().getName());
            FileUtils.writeToFile(filename, "Bolt inputs count: " + bolt.getInputsCount());
            FileUtils.writeToFile(filename, "Get bolt input list...");
            List<TopologyAPI.InputStream> inputStreams = bolt.getInputsList();
            for (TopologyAPI.InputStream inputStream : inputStreams) {
                FileUtils.writeToFile(filename, "Bolt input stream: " + inputStream);
                FileUtils.writeToFile(filename, "Bolt input stream id: " + inputStream.getStream().getId());
                FileUtils.writeToFile(filename, "This Stream's component name: " + inputStream.getStream().getComponentName());
            }
            FileUtils.writeToFile(filename, "Get bolt output list...");
            List<TopologyAPI.OutputStream> outputStreams = bolt.getOutputsList();
            for (TopologyAPI.OutputStream outputStream : outputStreams) {
                TopologyAPI.StreamId streamId = outputStream.getStream();
                FileUtils.writeToFile(filename, "Bolt output stream: " + streamId);
                FileUtils.writeToFile(filename, "Bolt Output Stream id: " + streamId.getId());
                FileUtils.writeToFile(filename, "This Stream's component name: " + streamId.getComponentName());
            }
        }
        FileUtils.writeToFile(filename, "-------------------------TOPOLOGY INFO END-------------------------");
    }

    private void outputTopologyInfo(TopologyAPI.Topology topology) {
        FileUtils.writeToFile(filename, "------------------------TOPOLOGY INFO START------------------------");
        FileUtils.writeToFile(filename, "Topology info is:" + topology.toString());
        FileUtils.writeToFile(filename, "------------------------TOPOLOGY INFO END------------------------");
    }

    /**
     * 20180705 copy from LaunchRunner.java
     * Trim the topology definition for storing into state manager.
     * This is because the user generated spouts and bolts
     * might be huge.
     *
     * @return trimmed topology
     */
    public TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {
        // create a copy of the topology physical plan
        TopologyAPI.Topology.Builder builder = TopologyAPI.Topology.newBuilder().mergeFrom(topology);
        // clear the state of user spout java objects - which can be potentially huge
        for (TopologyAPI.Spout.Builder spout : builder.getSpoutsBuilderList()) {
            spout.getCompBuilder().clearSerializedObject();
        }
        // clear the state of user spout java objects - which can be potentially huge
        for (TopologyAPI.Bolt.Builder bolt : builder.getBoltsBuilderList()) {
            bolt.getCompBuilder().clearSerializedObject();
        }
        return builder.build();
    }

    /**
     * @param runtime
     * @return
     * @throws SchedulerException
     */
    protected ISchedulerClient getSchedulerClient(Config runtime)
            throws SchedulerException {
        return new SchedulerClientFactory(config, runtime).getSchedulerClient();
    }


    protected void validateRuntimeManage(
            SchedulerStateManagerAdaptor adaptor,
            String topologyName) throws TopologyRuntimeManagementException {
        // Check whether the WordCountTopology has already been running
        Boolean isTopologyRunning = adaptor.isTopologyRunning(topologyName);

        if (isTopologyRunning == null || isTopologyRunning.equals(Boolean.FALSE)) {
            throw new TopologyRuntimeManagementException(
                    String.format("Topology '%s' does not exist", topologyName));
        }

        // Check whether cluster/role/environ matched
        ExecutionEnvironment.ExecutionState executionState = adaptor.getExecutionState(topologyName);
        if (executionState == null) {
            throw new TopologyRuntimeManagementException(
                    String.format("Failed to get execution state for WordCountTopology %s", topologyName));
        }

        String stateCluster = executionState.getCluster();
        String stateRole = executionState.getRole();
        String stateEnv = executionState.getEnviron();
        String configCluster = Context.cluster(config);
        String configRole = Context.role(config);
        String configEnv = Context.environ(config);
        if (!stateCluster.equals(configCluster)
                || !stateRole.equals(configRole)
                || !stateEnv.equals(configEnv)) {
            String currentState = String.format("%s/%s/%s", stateCluster, stateRole, stateEnv);
            String configState = String.format("%s/%s/%s", configCluster, configRole, configEnv);
            throw new TopologyRuntimeManagementException(String.format(
                    "cluster/role/environ does not match. Topology '%s' is running at %s, not %s",
                    topologyName, currentState, configState));
        }
    }
    /****************************************************************************/
    //                        End ReScheduler Tools Functions
    /****************************************************************************/
}
