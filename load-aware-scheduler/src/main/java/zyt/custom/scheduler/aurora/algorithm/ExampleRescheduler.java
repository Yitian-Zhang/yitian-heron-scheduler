package zyt.custom.scheduler.aurora.algorithm;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.LauncherUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import zyt.custom.scheduler.Constants;
import zyt.custom.scheduler.rescheduler.AuroraRescheduler;
import zyt.custom.scheduler.utils.SchedulerUtils;
import zyt.custom.scheduler.utils.TopologyInfoUtils;
import zyt.custom.utils.FileUtils;

/**
 * @author yitian
 */
public class ExampleRescheduler implements AuroraRescheduler {

    private static final String filename = Constants.SCHEDULER_LOG_FILE;;
    private Config config;
    private Config runtime;
    private TopologyAPI.Topology topology;
    private String topologyName;
    private PackingPlanProtoSerializer serializer;
    private PackingPlanProtoDeserializer deserializer;

    public ExampleRescheduler() {
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
     * Heron rescheduling function model
     *
     * @param packingPlan current packing plan
     */
    public void reschedule(PackingPlan packingPlan) {
        FileUtils.writeToFile(filename, "-----------------TRIGGER RESCHEDULER START-----------------");
        // 1. init StateManager instance
        String stateMgrClass = Context.stateManagerClass(this.config); // get state manager instance
        IStateManager stateMgr = null;

        try {
            stateMgr = ReflectionUtils.newInstance(stateMgrClass);
            FileUtils.writeToFile(filename, "Create IStateManager object success.");

            // 2. create StateManagerAdaptor instance
            stateMgr.initialize(this.config);
            SchedulerStateManagerAdaptor stateManagerAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);

            // 3. print current PhysicalPlan before rescheduling
            FileUtils.writeToFile(filename, "Before trigger scheduler, the physical plan is:");
            PhysicalPlans.PhysicalPlan physicalPlan = stateManagerAdaptor.getPhysicalPlan(topologyName);
            TopologyInfoUtils.getPhysicalPlanInfo(physicalPlan, filename);

            // 4. print current PackingPlan
            FileUtils.writeToFile(filename, "The packing algorithm class is: " + Context.packingClass(config));
            FileUtils.writeToFile(filename, "Before trigger scheduler, the packing plan is:");
            TopologyInfoUtils.printPackingInfo(packingPlan, filename);

            // 5. build your own packing plan based on your algorithm
            // In here, we modified the RR algorithm to FFDP algorithm when trigger a rescheduling for
            // demonstrating the process of rescheduling.
            /*--------------------Build the custom rescheduling algorithm start--------------------*/
            FileUtils.writeToFile(filename, "Create the new Config based on the FFDP packing algorithm.");
            // Attention: to use FFDP algorithm, you should modify the requested resources
            config = Config.newBuilder().putAll(config).put(Key.PACKING_CLASS,
                    "com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking").build();

            String packingClass = Context.packingClass(config);
            FileUtils.writeToFile(filename, "Then, packing algorithm is: " + packingClass); // there is no problem

            // build new packing plan using FFDP algorithm
            FileUtils.writeToFile(filename, "Create the new PackingPlan using New Packing Algorithm...");
            PackingPlan newPackingPlan = LauncherUtils.getInstance().createPackingPlan(config, runtime);
            FileUtils.writeToFile(filename, "Then, new packing plan info...");
            TopologyInfoUtils.printPackingInfo(newPackingPlan, filename);
            /*--------------------Build the custom rescheduling algorithm end--------------------*/

            // 6. serializer packing plan for creating updateTopologyRequest
            FileUtils.writeToFile(filename, "Now, Update topology using updateTopologyManager...");
            PackingPlans.PackingPlan currentPackingPlan = stateManagerAdaptor.getPackingPlan(topologyName);
            PackingPlans.PackingPlan proposedPackingPlan = serializer.toProto(newPackingPlan);
            // no need to validate topology
            // validateRuntimeManage(stateManagerAdaptor, topologyName);

            // 7. create SchedulerClient for updateTopology
            Config newRuntime = Config.newBuilder()
                    .put(Key.TOPOLOGY_NAME, Context.topologyName(config))
                    .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, stateManagerAdaptor)
                    .build();
            ISchedulerClient schedulerClient = SchedulerUtils.getSchedulerClient(newRuntime, config);

            // 8. build updateTopologyRequest object to update topology
            Scheduler.UpdateTopologyRequest updateTopologyRequest =
                    Scheduler.UpdateTopologyRequest.newBuilder()
                            .setCurrentPackingPlan(currentPackingPlan)
                            .setProposedPackingPlan(proposedPackingPlan)
                            .build();
            FileUtils.writeToFile(filename, "Sending Updating Topology request: " + updateTopologyRequest);
            if (!schedulerClient.updateTopology(updateTopologyRequest)) {
                throw new TopologyRuntimeManagementException(String.format(
                        "Failed to update " + topology.getName() + " with Scheduler, updateTopologyRequest="
                                + updateTopologyRequest));
            }

            FileUtils.writeToFile(filename, "Schedule update topology successfully!");

            FileUtils.writeToFile(filename, "After trigger scheduler, the physical plan is:");
            TopologyInfoUtils.getPhysicalPlanInfo(stateManagerAdaptor.getPhysicalPlan(topologyName), filename);
            FileUtils.writeToFile(filename, "-----------------TRIGGER RESCHEDULER END-----------------");
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // 9. clean the connection when we are done.
            SysUtils.closeIgnoringExceptions(stateMgr);
        }
    }
}
