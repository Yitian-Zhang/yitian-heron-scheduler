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
public class LoadAwareRescheduler implements AuroraRescheduler {

    private static final Logger logger = Logger.getLogger(LoadAwareRescheduler.class.getName());

    // basic variable
    private static final String filename = Constants.SCHEDULER_LOG_FILE;;
    private Config config;
    private Config runtime;
    private TopologyAPI.Topology topology;
    private String topologyName;
    private PackingPlanProtoSerializer serializer;
    private PackingPlanProtoDeserializer deserializer;

    // addition
    private ByteAmount containerRamPadding = Constants.DEFAULT_RAM_PADDING_PER_CONTAINER;


    public LoadAwareRescheduler() {

    }

    public void initialize(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;
        this.topology = Runtime.topology(this.runtime);
        this.topologyName = Runtime.topologyName(this.runtime);
        this.serializer = new PackingPlanProtoSerializer();
        this.deserializer = new PackingPlanProtoDeserializer();
    }

    /**
     * Load-aware online scheduling algorithm
     * 2018-09-16 add
     *
     * @param packingPlan original packingplan
     */
    @Override
    public void reschedule(PackingPlan packingPlan) {
        logger.info("-----------------BASED WEIGHT RESCHEDULING ALGORITHM START-----------------");
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
            logger.info("[CORE] - Current packing algorithm is: " + Context.packingClass(this.config));
            // physcial plan
            logger.info("[CORE] - Current physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(this.topologyName);
            TopologyInfoUtils.getPhysicalPlanInfo(physicalPlan, filename);
            // packing plan
            logger.info("[CORE] - Current packing plan is:");
            TopologyInfoUtils.printPackingInfo(packingPlan, filename);

            // **************************NEED TO CUSTOM ALGORITHM START***************************
            logger.info("*****************CREATE NEW PACKINGPLAN USING CUSTOM ALGORITHM*****************");
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
            logger.info("[BEFORE] - SpoutInstanceList: " + Utils.collectionToString(spoutInstanceList));

            List<Integer> boltInstanceList = getBoltInstances(topology, instancesList);
            logger.info("[BEFORE] - BoltInstanceList: " + Utils.collectionToString(boltInstanceList));

            List<Integer> allInstanceIdList = new ArrayList<>();
            allInstanceIdList.addAll(spoutInstanceList);
            allInstanceIdList.addAll(boltInstanceList);
            logger.info("[BEFORE] - ALlInstanceList: " + Utils.collectionToString(allInstanceIdList));

            // new task assignment allocation
            logger.info("[CURRENT] - Create newTaskToStmgr: stmgr->taskList to build new ContainerPlan...");
            Map<String, List<Integer>> newTaskToStmgr = new HashMap<>();
            String lastStmgrId = "";
            for (String stmgrId : taskToStmgr.keySet()) {
                logger.info("[INITIALIZE] - Initialize the stmgrId is: " + stmgrId);
                if (newTaskToStmgr.get(stmgrId) == null) {
                    newTaskToStmgr.put(stmgrId, new ArrayList<Integer>());
                }
                lastStmgrId = stmgrId;
            }
            logger.info("[INITIALIZE] - Initialize stmgrId: " + lastStmgrId + " with all instances: " + Utils.collectionToString(allInstanceIdList));
            newTaskToStmgr.get(lastStmgrId).addAll(allInstanceIdList); // first assigned all task to the last stmgr

            // Based on Weight Scheduling Algorithm
            logger.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM START********************");
            // calculating cluster load W, and average W of all worker node
            Long allWeight = getWeightOfCluster(taskLoadMap);
            // get instance number and calculate the average cpu load
            Long averageWeight = allWeight / taskToStmgr.keySet().size(); // the average weight of each worker node(stmgr) - expected weight value
            logger.info("Calculate Result: [AllWeight: " + allWeight + "], [AverageWeight: " + averageWeight + "]");

//            if (stmgrToHostMap.keySet().size() == 1) {
//                logger.info("[ATTENTION] - There is only one worker node(stmgr) in this cluster, don't processe this BW-Algorithm!");
//                logger.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM END********************");
//                return;
//            } else {
//                logger.info("[ATTENTION] - There is " + stmgrToHostMap.keySet().size() +" worker node(stmgr) in this cluster, can process BW-Algorithm...");
//            }

            // foreach worknode in the cluster
            for (String hostname : stmgrToHostMap.keySet()) {
                List<String> stmgrList = stmgrToHostMap.get(hostname);
                for (String stmgrId : stmgrList) { // only one stmgr in one worker node
                    if (stmgrId.equals(lastStmgrId)) {
                        logger.info("[BREAK] - Current stmgr is the last stmgr: " + stmgrId + ", so break this loop!");
                        break;
                    }

                    logger.info("[CURRENT] - Assigning tasks to [hostname: " + hostname + ", stmgrId: " + stmgrId + "]");
                    // get current stmgr's cpu load
                    long stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                    logger.info("[CURRENT] - This stmgr initialize load weight is: " + stmgrWeight); // stmgrWeight will changed
                    // get current last stmgr task list
                    List<Integer> lastStmgrTaskList = newTaskToStmgr.get(lastStmgrId);

                    // 20181015 add instances number check ------------------------------
                    int instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                    logger.info("[CURRENT] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] instances. ");
                    // ------------------------------------------------------------------

                    // random choose one bolt or one spout when assigned one stmgr start
                    boolean hasBolt = hasBoltInLastStmgr(boltInstanceList, lastStmgrTaskList); // has bolt: true, don't has bolt: false
                    Integer willAssignmentTaskId = null;

                    // ##20181108 find max load task in the current last stmgr (unassignment task list)
                    willAssignmentTaskId = getMaxWorkloadInstanceInLastStmgr(taskLoadMap, newTaskToStmgr.get(lastStmgrId));
                    logger.info("[##] - Current max workload task in the last stmgr is: " + willAssignmentTaskId);
//                    if (!hasBolt) { // last stmgr do not has bolt
//                        for (Integer lastStmgrTaskId : lastStmgrTaskList) {
//                            if (spoutInstanceList.contains(lastStmgrTaskId)) {
//                                logger.info(" - There is a spout in last stmgr, its taskid is: " + lastStmgrTaskId);
//                                willAssignmentTaskId = lastStmgrTaskId;
//                                break;
//                            }
//                        }
//                    } else { // last stmgr do has bolt
//                        for (Integer lastStmgrTaskId : lastStmgrTaskList) {
//                            if (boltInstanceList.contains(lastStmgrTaskId)) {
//                                logger.info(" - There is a bolt in last stmgr, its taskId is: " + lastStmgrTaskId);
//                                willAssignmentTaskId = lastStmgrTaskId;
//                                break;
//                            }
//                        }
//                    }
                    // update some value
                    if (willAssignmentTaskId != null) {
                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        logger.info("[CHECK] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        if (instanceNumberInStmgr >= 4) {
                            break;
                        }
                        // ------------------------------------------------------------------

                        logger.info("[CURRENT] - WillAssignmentTaskId is: " + willAssignmentTaskId);
                        newTaskToStmgr.get(stmgrId).add(willAssignmentTaskId);
                        newTaskToStmgr.get(lastStmgrId).remove(willAssignmentTaskId);
                        logger.info("[CURRENT] - Current NewTaskToStmgr is: ");
                        outputTaskToStmgr(newTaskToStmgr);

                        stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                        logger.info("[CURRENT] - stmgrWeight is: " + stmgrWeight);
                    } else {
                        logger.info("[ATTENTION] - WillAssignmentTaskId is null!!! There is no task to assignment!!!");
                    }

                    // if there is only one stmgr(worker node), this whlie loop will not processing, because the stmgrweight will always > the averageWeight
                    // but if change < to <=, the while loop should will be processing. try it.
                    // now, there is only stmgr, so not to test following codes in while loop
//                    while (stmgrWeight < averageWeight && instanceNumberInStmgr <= 4) { // the condition for ending this loop
                    while (instanceNumberInStmgr <= 4) { // the condition for ending this loop

                        logger.info("[CURRENT] - This stmgr current load weight is: " + stmgrWeight + " and AverageWeight is: " + averageWeight); // stmgrWeight will changed

                        /*************CALCULATE EdgeWeightGain START**************/
                        logger.info("[EWG] - Calculating EdgeWeightGain of this willAssignmentTaskId: " + willAssignmentTaskId + " 's connected tasks...");
                        // ### Modified by yitian 2018-10-19 ---------------------------
                        // create a new list, it includes all tasks in current stmgr, and get the task with min EWG value in this list, not only the task with min EWG in the task list of current task connected
                        List<Integer> allTaskOfCurrentStmgr = new ArrayList<>();
                        for (int taskId : newTaskToStmgr.get(stmgrId)) {
                            List<Integer> connectedTaskList = getConnectedTasksInLastStmgr(taskId, pairList, newTaskToStmgr.get(lastStmgrId));
                            logger.info("[CONNECTED-TASK] - The WillAssignmentTaskId: " + taskId + "'s connectedTaskList is: " + Utils.collectionToString(connectedTaskList));
                            allTaskOfCurrentStmgr.addAll(connectedTaskList);
                        }
                        // get connection task list of this willAssignmentTask
//                        List<Integer> connectedTaskList = getConnectedTasksInLastStmgr(willAssignmentTaskId, pairList, newTaskToStmgr.get(lastStmgrId));
//                        logger.info("The WillAssignmentTaskId: " + willAssignmentTaskId + "'s connectedTaskList is: " + Utils.collectionToString(connectedTaskList));
                        //
                        logger.info("[CONNECTED-TASK] - The CurrentStmgr: " + stmgrId + "'s allTaskOfCurrentStmgr is: " + Utils.collectionToString(allTaskOfCurrentStmgr));
                        // ### Modified by yitian 2018-10-19 ---------------------------
                        // foreach connectedTaskId in connectedTaskList
                        int maxEWG = Integer.MIN_VALUE;
                        int showWillAssignmentTaskId = willAssignmentTaskId; // record the willAssignmentTaskId to show
                        for (Integer connectedTaskId : allTaskOfCurrentStmgr) { // ### modified connectedTaskList -> allTaskOfCurrentStmgr
                            // calculating the pair list in current stmgr and last stmgr
                            List<ExecutorPair> pairListInCurrentStmgr = getConnectedTaskInStmgr(connectedTaskId, pairList, newTaskToStmgr.get(stmgrId));
                            logger.info("[CURRENT] - Connected task: " + connectedTaskId + " of this taskId in stmgr_(" + stmgrId + ") is: " + Utils.collectionToString(pairListInCurrentStmgr));
                            // then getConnectedTaskInLastStmgr
                            List<ExecutorPair> pairListInLastStmgr = getConnectedTaskInStmgr(connectedTaskId, pairList, newTaskToStmgr.get(lastStmgrId));
                            logger.info("[CURRENT] - Connected task " + connectedTaskId + " of this taskId in lastStmgr_(" + lastStmgrId + ") is: " + Utils.collectionToString(pairListInLastStmgr));

                            // calculating Edge Weight Gain of each connectedTaskId
                            // get Max(Edge Weight Gain) task to assign to this current stmgrId
                            int edgeWeightGain = calculateEdgeWeightGain(pairListInCurrentStmgr, pairListInLastStmgr);// return int value
                            logger.info("- Current task( " + showWillAssignmentTaskId + " ) has connected task: " + connectedTaskId + ", its EWG is: " + edgeWeightGain);
                            if (edgeWeightGain > maxEWG) { // get max EWG of connected task
                                maxEWG = edgeWeightGain;
                                willAssignmentTaskId = connectedTaskId; // update the value of willAssignmentTaskId for next while loop
                            }
                            logger.info("- And maxEWG: " + maxEWG + ". Then current willAssignmentTask is: " + willAssignmentTaskId);
                        }

                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        logger.info("[CHECK] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        if (instanceNumberInStmgr >= 4) {
                            break;
                        }
                        // ------------------------------------------------------------------
                        // assignment willAssignmentTaskId's connected tasks
                        // update some value
                        newTaskToStmgr.get(stmgrId).add(willAssignmentTaskId); // will change this willAssignmentTaskId
                        newTaskToStmgr.get(lastStmgrId).remove(willAssignmentTaskId);
                        logger.info("[CURRENT] - Current NewTaskToStmgr is: ");
                        outputTaskToStmgr(newTaskToStmgr);
                        /*************CALCULATE EdgeWeightGain END**************/

                        // update stmgr weight
                        stmgrWeight = getLoadForStmgr(stmgrId, stmgrToHostMap, newTaskToStmgr, taskLoadMap);
                        logger.info("[CURRENT] - StmgrWeight is: " + stmgrWeight);

                        // 20181015 add instances number check ------------------------------
                        instanceNumberInStmgr = getTaskNumInNewStmgr(stmgrId, newTaskToStmgr);
                        logger.info("[UPDATE] - This stmgr: [" + stmgrId + "] has [" + instanceNumberInStmgr + "] intances. ");
                        // ------------------------------------------------------------------

                    }
                }
            }
            logger.info("********************BASED ON WEIGHT SCHEDULING ALGORITHM END********************");

            // output the newTaskToStmgr
            logger.info("[AFTER] - The newTaskToStmgr is: ");
            outputTaskToStmgr(newTaskToStmgr);

            // above content wiil be changed ---------------- 2018-09-17 before go to Tianshan
            // get new allocation
            logger.info("[THEN] - Get tasks allocation using newTaskToStmgr...");
            Map<Integer, List<InstanceId>> newAllocation = getAllocationBasedAlgorithm(newTaskToStmgr, physicalPlan);
            Set<PackingPlan.ContainerPlan> newContainerPlans = new HashSet<>();

            logger.info("[THEN] - Creating new containerPlan...");
            for (int containerId : newAllocation.keySet()) {
                List<InstanceId> instanceIdList = newAllocation.get(containerId);
                //
                Map<InstanceId, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();
                // unused in this class
                ByteAmount containerRam = containerRamPadding;
                logger.info("- Getting instance resource from packing plan...");
                for (InstanceId instanceId : instanceIdList) {
                    Resource resource = TopologyInfoUtils.getInstanceResourceFromPackingPlan(instanceId.getTaskId(), packingPlan);
                    if (resource != null) {
                        instancePlanMap.put(instanceId, new PackingPlan.InstancePlan(instanceId, resource));
                    } else {
                        logger.info("Getting instance resource error!!!");
                    }
                }

                PackingPlan.ContainerPlan newContainerPlan = null;
                logger.info("- Get container resource from packing plan...");
                Resource containerRequiredResource = TopologyInfoUtils.getContainerResourceFromPackingPlan(containerId, packingPlan);
                if (containerRequiredResource != null) {
                    newContainerPlan = new PackingPlan.ContainerPlan(containerId, new HashSet<>(instancePlanMap.values()), containerRequiredResource);
                } else {
                    logger.info("Getting container resource error!!!");
                }
                newContainerPlans.add(newContainerPlan);
            }

            logger.info("[NOW] - Creating new packing plan...");
            PackingPlan basedWeightPackingPlan = new PackingPlan(topology.getId(), newContainerPlans);
            logger.info("[NOW] - Validate Packing plan...");
            SchedulerUtils.validatePackingPlan(basedWeightPackingPlan);

            logger.info("=========================CREATED PACKING PLAN SUCCESSED=========================");
//            outputPackingInfoToScheduleLog(basedWeightPackingPlan);

            logger.info("[NOW] - Update topology with new packing by updateTopologyManager...");
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(basedWeightPackingPlan);

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

            logger.info("[CORE] - Update topology using updateTopologyManager...");
            logger.info("[CORE] - Sending updating topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "[CORE] - Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            // Clean the connection when we are done.
            logger.info("[CORE] - =========================UPDATE TOPOLOGY SUCCESSFULLY=========================");
            logger.info("[CORE] - After trigger scheduler, the physical plan is:");
            TopologyInfoUtils.getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName), filename);

            logger.info("-----------------BASED WEIGHT RESCHEDULING ALGORITHM END-----------------");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // close zookeeper client connnection
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }

    /*-------------------------------------------------------------------------------------------*/
    /*-----------------------Based Weight Scheduling Algorithm Tools Start-----------------------*/
    /*-------------------------------------------------------------------------------------------*/

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
        List<Integer> stmgrTaskListTemp = Utils.deepCopyIntegerArrayList(stmgrTaskList);

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
//        logger.info("[FUNCTION] - stmgrTaskListTemp: " + Utils.collectionToString(stmgrTaskListTemp));

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
        logger.info("[FUNCTION]----------------CREATE ALLOCATION BASED ON CUSTOM ALGORITHM START----------------");
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
            logger.info("- Container: " + containerId + " has new allocation: " + Utils.collectionToString(allocation.get(containerId)));
        }
        logger.info("[FUNCTION]----------------CREATE ALLOCATION BASED ON CUSTOM ALGORITHM END----------------");
        return allocation;
    }

    /**
     * output the taskToStmgr map info
     *
     * @param newTaskToStmgr
     */
    private void outputTaskToStmgr(Map<String, List<Integer>> newTaskToStmgr) {
        logger.info("[FUNCTION]------------OUTPUT STMGR->TASKS START------------");
        for (String stmgrId : newTaskToStmgr.keySet()) {
            logger.info("NewTaskToStmgr: " + stmgrId + " has tasks: " + Utils.collectionToString(newTaskToStmgr.get(stmgrId)));
        }
        logger.info("[FUNCTION]------------OUTPUT STMGR->TASKS END------------");
    }

    /**
     * @param willAssignmentTaskId
     * @param connectedTaskId
     * @param pairList
     */
    private void updateExecutorPairList(Integer willAssignmentTaskId, Integer connectedTaskId, List<ExecutorPair> pairList) {
        logger.info("-------------------------UPDATE EXECUTOR PAIR LIST START-------------------------");
        logger.info("[FUNCTION] - Update Executor Pair List with: [" + willAssignmentTaskId + ", " + connectedTaskId + "]");
        for (ExecutorPair pair : pairList) {
            int sourceTaskId = pair.getSource().getBeginTask();
            int destinationTaskId = pair.getDestination().getBeginTask();
            if ((sourceTaskId == willAssignmentTaskId && destinationTaskId == connectedTaskId) || (sourceTaskId == connectedTaskId && destinationTaskId == willAssignmentTaskId)) {
                logger.info("- Will remove pair from ExecutorPairList is: " + pair);
                pairList.remove(pair);
            }
        }
        logger.info("-------------------------UPDATE EXECUTOR PAIR LIST END-------------------------");
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

        logger.info("[FUNCTION] - Current task list in last stmgr is: " + Utils.collectionToString(lastStmgrTaskList));
        for (int taskId : lastStmgrTaskList) {
            long workload = taskLoadMap.get(taskId);
            logger.info("[FUNCTION] - Current taskId: " + taskId + ", workload: " + workload);
            if (workload > maxWorkLoad) {
                maxWorkLoad = workload;
                taskIdOfMaxLoad = taskId;
            }
        }
        return taskIdOfMaxLoad;
    }

    /*-----------------------------------------------------------------------------------------*/
    /*-----------------------Based Weight Scheduling Algorithm Tools End-----------------------*/
    /*-----------------------------------------------------------------------------------------*/


    // common functions (used in Load-aware and dsc-heron)
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

    /**
     * @param physicalPlan
     * @return
     */
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

    private List<PhysicalPlans.Instance> getInstancesList(PhysicalPlans.PhysicalPlan physicalPlan) {
        logger.info("Getting instance list from physical plan...");
        List<PhysicalPlans.Instance> instanceList = physicalPlan.getInstancesList();
        return instanceList;
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
    private long getLoadForStmgr(String stmgrId, Map<String, List<String>> stmgrToHostMap, Map<String,
            List<Integer>> newTaskToStmgr, Map<Integer, Long> taskLoadMap) {
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

    private int getTaskNumInNewStmgr(String stmgrId, Map<String, List<Integer>> newTaskToStmgr) {
        int resultCount = 0;
        List<Integer> taskListOfStmgr = newTaskToStmgr.get(stmgrId);
        if (taskListOfStmgr.size() != 0) {
            resultCount = taskListOfStmgr.size();
        }
        return resultCount;
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

}
