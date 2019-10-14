package zyt.custom.scheduler.utils;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import zyt.custom.utils.FileUtils;
import zyt.custom.utils.Utils;

import java.util.*;

public class TopologyInfoUtils {

    /*-------------------Get Topology Basic Info--------------------*/

    /**
     * Print the PhysicalPlan of current topology to identified file
     *
     * @return PhysicalPlans.PhysicalPlan
     */
    public static PhysicalPlans.PhysicalPlan getPhysicalPlanInfo(PhysicalPlans.PhysicalPlan physicalPlan, String filename) {
        FileUtils.writeToFile(filename, "[FUNCTION]----------------GET PHYSICAL PLAN START----------------");
        FileUtils.writeToFile(filename, "Getting hosts list of word nodes from physical plan...");
        List<String> hostList = new ArrayList<>(); // hostname list in this topology
        for (PhysicalPlans.StMgr stMgr : physicalPlan.getStmgrsList()) { // get hostanem from Stmgr class
            String hostname = stMgr.getHostName();
            if (!hostList.contains(hostname)) {
                hostList.add(hostname);
            }
        }
        FileUtils.writeToFile(filename, "Hostlist: " + Utils.collectionToString(hostList));

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
            FileUtils.writeToFile(filename, "Hostname: " + hostname + " stmgr list: " + Utils.collectionToString(stmgrToHostMap.get(hostname)));
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
     * Copied from LaunchRunner.java
     * Trim the topology definition for storing into state manager.
     * This is because the user generated spouts and bolts might be huge.
     *
     * @return trimmed topology
     */
    public static TopologyAPI.Topology trimTopology(TopologyAPI.Topology topology) {
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
     * Simply print topology info to file
     *
     * @param topology topology
     * @param filename file to print
     */
    public static void printTopologyInfoSimply(TopologyAPI.Topology topology, String filename) {
        FileUtils.writeToFile(filename, "------------------------TOPOLOGY INFO START------------------------");
        FileUtils.writeToFile(filename, "Topology info is:" + topology.toString());
        FileUtils.writeToFile(filename, "------------------------TOPOLOGY INFO END------------------------");
    }

    /**
     * Detailed print topology info to file
     *
     * @param runtime get topology from the runtime
     * @param filename file to print
     */
    private void printTopologyInfoDetial(Config runtime, String filename) {
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

    /**
     * Print packing info to file
     *
     * @param packingPlan current packing plan
     * @param filename file to print
     */
    public static void printPackingInfo(PackingPlan packingPlan, String filename) {
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




}
