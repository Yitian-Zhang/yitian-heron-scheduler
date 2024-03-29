// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zyt.custom.scheduler.aurora;

import com.google.common.base.Optional;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.UpdateTopologyManager;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.common.TokenSub;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.scheduler.IScalable;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import zyt.custom.scheduler.aurora.common.*;
import zyt.custom.utils.FileUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AuroraCustomScheduler
 *
 * 20180718 for rescheduler-core ---------------------
 * The content in this, which is the custom code for
 * custom scheduler.
 * ---------------------------------------------------
 *
 * @author yitian
 *
 */
public class AuroraCustomScheduler implements IScheduler, IScalable {

    private static final Logger LOG = Logger.getLogger(AuroraCustomScheduler.class.getName());

    private static final String FILE_NAME = "/home/yitian/logs/aurora-scheduler/aurora-scheduler.txt";

    private Config config;

    private Config runtime;

    private AuroraController controller;

    private UpdateTopologyManager updateTopologyManager;

    // 20180718 for rescheduler-core ---------------------
    private AuroraSchedulerController schedulerController;

    private TopologyAPI.Topology topology;
    // ---------------------------------------------------

    @Override
    public void initialize(Config mConfig, Config mRuntime) {
        this.config = Config.toClusterMode(mConfig);
        this.runtime = mRuntime;
        try {
            this.controller = getController();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.severe("AuroraController initialization failed " + e.getMessage());
        }

        this.updateTopologyManager = new UpdateTopologyManager(config, runtime, Optional.<IScalable>of(this));

        // 0718 for rescheduler-core --------------------------------------
        this.topology = Runtime.topology(this.runtime);
        this.schedulerController = new AuroraSchedulerController(Config.toLocalMode(this.config), this.runtime);
        // ----------------------------------------------------------------
    }

    /**
     * Get an AuroraController based on the config and runtime
     *
     * @return AuroraController
     */
    protected AuroraController getController()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        // .heron/conf/aurora/scheduler.yaml: # heron.class.scheduler.aurora.controller.cli: False
        Boolean cliController = config.getBooleanValue(Key.AURORA_CONTROLLER_CLASS);
        Config localConfig = Config.toLocalMode(this.config);
        if (cliController) {
            return new AuroraCLIController(
                    Runtime.topologyName(runtime),
                    Context.cluster(localConfig),
                    Context.role(localConfig),
                    Context.environ(localConfig),
                    AuroraContext.getHeronAuroraPath(localConfig),
                    Context.verbose(localConfig));
        } else {
            return new AuroraHeronShellController(
                    Runtime.topologyName(runtime),
                    Context.cluster(localConfig),
                    Context.role(localConfig),
                    Context.environ(localConfig),
                    AuroraContext.getHeronAuroraPath(localConfig),
                    Context.verbose(localConfig),
                    localConfig);
        }
    }

    @Override
    public void close() {
        if (updateTopologyManager != null) {
            updateTopologyManager.close();
        }
    }

    @Override
    public boolean onSchedule(PackingPlan packing) {
        if (packing == null || packing.getContainers().isEmpty()) {
            LOG.severe("No container requested. Can't schedule");
            return false;
        }

        LOG.info("Launching topology in aurora");

        // Align the cpu, ram, disk to the maximal one, and set them to ScheduledResource
        PackingPlan updatedPackingPlan = packing.cloneWithHomogeneousScheduledResource();
        SchedulerUtils.persistUpdatedPackingPlan(Runtime.topologyName(runtime), updatedPackingPlan,
                Runtime.schedulerStateManagerAdaptor(runtime));

        // Use the ScheduledResource to create aurora properties
        // the ScheduledResource is guaranteed to be set after calling
        // cloneWithHomogeneousScheduledResource in the above code
        Resource containerResource =
                updatedPackingPlan.getContainers().iterator().next().getScheduledResource().get(); // 获取container的resources
        Map<AuroraField, String> auroraProperties = createAuroraProperties(containerResource);

        boolean isSuccess = controller.createJob(auroraProperties);
        // 0718 for rescheduler-core --------------------------------------
        new AuroraSchedulerThread(topology.getName(), packing, schedulerController).start();
        // ----------------------------------------------------------------
        return isSuccess;
    }

    @Override
    public List<String> getJobLinks() {
        List<String> jobLinks = new ArrayList<>();

        //Only the aurora job page is returned
        String jobLinkFormat = AuroraContext.getJobLinkTemplate(config);
        if (jobLinkFormat != null && !jobLinkFormat.isEmpty()) {
            String jobLink = TokenSub.substitute(config, jobLinkFormat);
            jobLinks.add(jobLink);
        }

        return jobLinks;
    }

    @Override
    public boolean onKill(Scheduler.KillTopologyRequest request) {
        // The aurora service can be unavailable or unstable for a while,
        // we will try to kill the job with multiple attempts
        int attempts = AuroraContext.getJobMaxKillAttempts(config);
        long retryIntervalMs = AuroraContext.getJobKillRetryIntervalMs(config);
        LOG.info("Will try " + attempts + " attempts at interval: " + retryIntervalMs + " ms");

        // First attempt
        boolean res = controller.killJob();
        attempts--;

        // Failure retry
        while (!res && attempts > 0) {
            LOG.warning("Failed to kill the topology. Will retry in " + retryIntervalMs + " ms...");
            Utils.sleep(retryIntervalMs);

            // Retry the killJob()
            res = controller.killJob();
            attempts--;
        }

        return res;
    }

    @Override
    public boolean onRestart(Scheduler.RestartTopologyRequest request) {
        Integer containerId = null;
        if (request.getContainerIndex() != -1) {
            containerId = request.getContainerIndex();
        }
        return controller.restart(containerId);
    }

    /**
     * For rescheduler-core
     *
     * @param request udpateRequest
     * @return
     */
    @Override
    public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
        FileUtils.writeToFile(FILE_NAME, "--------------IN SCHEDULER ONUPDATE FUNCTION START--------------");
        SchedulerStateManagerAdaptor adaptor = Runtime.schedulerStateManagerAdaptor(this.runtime);
        FileUtils.writeToFile(FILE_NAME, "This runtime adaptor is: " + adaptor.toString());
        try {
            updateTopologyManager.updateTopology(
                    request.getCurrentPackingPlan(), request.getProposedPackingPlan());
        } catch (ExecutionException | InterruptedException e) {
            LOG.log(Level.SEVERE, "Could not update topology for request: " + request, e);
            return false;
        }
        FileUtils.writeToFile(FILE_NAME, "--------------IN SCHEDULER ONUPDATE FUNCTION END--------------");
        return true;
    }

    /**
     * updated for 0.17.5 version
     */
    @Override
    public Set<PackingPlan.ContainerPlan> addContainers(
            Set<PackingPlan.ContainerPlan> containersToAdd) {
        // Do the actual containers adding
        LinkedList<Integer> newAddedContainerIds = new LinkedList<>(
                controller.addContainers(containersToAdd.size()));
        if (newAddedContainerIds.size() != containersToAdd.size()) {
            throw new RuntimeException(
                    "Aurora returned differnt countainer count " + newAddedContainerIds.size()
                            + "; input count was " + containersToAdd.size());
        }

        Set<PackingPlan.ContainerPlan> remapping = new HashSet<>();
        // Do the remapping:
        // use the `newAddedContainerIds` to replace the container id in the `containersToAdd`
        for (PackingPlan.ContainerPlan cp : containersToAdd) {
            PackingPlan.ContainerPlan newContainerPlan =
                    new PackingPlan.ContainerPlan(
                            newAddedContainerIds.pop(), cp.getInstances(),
                            cp.getRequiredResource(), cp.getScheduledResource().orNull());
            remapping.add(newContainerPlan);
        }
        LOG.info("The remapping structure: " + remapping);
        return remapping;
    }

    @Override
    public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
        controller.removeContainers(containersToRemove);
    }

    /**
     * updated for 0.17.5 version
     */
    protected Map<AuroraField, String> createAuroraProperties(Resource containerResource) {
        Map<AuroraField, String> auroraProperties = new HashMap<>();

        TopologyAPI.Topology topology = Runtime.topology(runtime);

        auroraProperties.put(AuroraField.EXECUTOR_BINARY,
                Context.executorBinary(config));

        List<String> topologyArgs = new ArrayList<>();
        SchedulerUtils.addExecutorTopologyArgs(topologyArgs, config, runtime);
        String args = String.join(" ", topologyArgs);
        auroraProperties.put(AuroraField.TOPOLOGY_ARGUMENTS, args);

        auroraProperties.put(AuroraField.CLUSTER, Context.cluster(config));
        auroraProperties.put(AuroraField.ENVIRON, Context.environ(config));
        auroraProperties.put(AuroraField.ROLE, Context.role(config));
        auroraProperties.put(AuroraField.TOPOLOGY_NAME, topology.getName());

        auroraProperties.put(AuroraField.CPUS_PER_CONTAINER,
                Double.toString(containerResource.getCpu()));
        auroraProperties.put(AuroraField.DISK_PER_CONTAINER,
                Long.toString(containerResource.getDisk().asBytes()));
        auroraProperties.put(AuroraField.RAM_PER_CONTAINER,
                Long.toString(containerResource.getRam().asBytes()));

        auroraProperties.put(AuroraField.NUM_CONTAINERS,
                Integer.toString(1 + TopologyUtils.getNumContainers(topology)));

        // Job configuration attribute 'production' is deprecated.
        // Use 'tier' attribute instead
        // See: http://aurora.apache.org/documentation/latest/reference/configuration/#job-objects
        if ("prod".equals(Context.environ(config))) {
            auroraProperties.put(AuroraField.TIER, "preferred");
        } else {
            auroraProperties.put(AuroraField.TIER, "preemptible");
        }

        String heronCoreReleasePkgURI = Context.corePackageUri(config);
        String topologyPkgURI = Runtime.topologyPackageUri(runtime).toString();

        auroraProperties.put(AuroraField.CORE_PACKAGE_URI, heronCoreReleasePkgURI);
        auroraProperties.put(AuroraField.TOPOLOGY_PACKAGE_URI, topologyPkgURI);

        return auroraProperties;
    }
}
