package zyt.custom.scheduler.utils;

import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingException;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.SchedulerException;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import zyt.custom.scheduler.Constants;
import zyt.custom.utils.FileUtils;

public class SchedulerUtils {

    /*---------------Scheduling Functions----------------*/
    public static void validateRuntimeManage(
            SchedulerStateManagerAdaptor adaptor,
            String topologyName, Config config) throws TopologyRuntimeManagementException {
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

    /**
     * Output config and runtime info to log file
     */
    public static void outputConfigInfo(Config config, Config runtime, String filename) {
        FileUtils.writeToFile(filename, "--------------------CONFIG INFO START----------------------");
        FileUtils.writeToFile(filename, config.toString());
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO START---------------------");
        FileUtils.writeToFile(filename, runtime.toString());
        FileUtils.writeToFile(filename, "--------------------CONFIG AND RUNTIME INFO END--------------------------");
    }

    public static void outputRuntimeInfo(Config config, String filename) {
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO START----------------------");
        FileUtils.writeToFile(filename, config.toString());
        FileUtils.writeToFile(filename, "--------------------RUNTIME INFO END--------------------------");
    }


    public static ISchedulerClient getSchedulerClient(Config runtime, Config config)
            throws SchedulerException {
        return new SchedulerClientFactory(config, runtime).getSchedulerClient();
    }

    /**
     * ****************************************
     * add: 2081-05-21 from packing algorithm
     * ****************************************
     * Check whether the PackingPlan generated is valid
     * 这里只是对最后生成的InstanceRam资源是否小于MIN_RAM_PER_INSTANCE进行判断
     *
     * @param plan
     */
    public static void validatePackingPlan(PackingPlan plan) throws PackingException {
        for (PackingPlan.ContainerPlan containerPlan : plan.getContainers()) {
            for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
                // safe check
                if (instancePlan.getResource().getRam().lessThan(Constants.MIN_RAM_PER_INSTANCE)) {
                    throw new PackingException(String.format("Invalid packing plan generated. A minimum of "
                                    + "%s ram is required, but InstancePlan for component '%s' has %s",
                            Constants.MIN_RAM_PER_INSTANCE, instancePlan.getComponentName(),
                            instancePlan.getResource().getRam()));
                }
            }
        }
    }
}
