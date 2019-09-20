package zyt.custom.scheduler;

import com.twitter.heron.spi.packing.PackingPlan;
import org.apache.log4j.Logger;
import zyt.custom.tools.Utils;

import java.util.*;

/**
 * This class is not used.
 *
 */
public class AssignmentTracker {

    /**
     * containerid -> list<taskid>
     */
    private Map<Integer, List<String>> lastAssignment;

    private Logger logger = Logger.getLogger(AssignmentTracker.class);

    public void checkAssignment(PackingPlan packingPlan) {
        Map<Integer, List<String>> assignment = new HashMap<>();
        // get packing plan id. unused
        String packingPlanId = packingPlan.getId();

        // get containerplan map
        Map<Integer, PackingPlan.ContainerPlan> containerPlanMap = packingPlan.getContainersMap();

        for (Integer containerId : containerPlanMap.keySet()) {
            // get current container plan
            PackingPlan.ContainerPlan containerPlan = containerPlanMap.get(containerId);
            Set<PackingPlan.InstancePlan> instancePlanSet = containerPlan.getInstances();

            for (PackingPlan.InstancePlan instancePlan : instancePlanSet) {
                String instancePlanDescription = instancePlan.toString();

                List<String> instancesList = assignment.get(containerId);
                if (instancesList == null) {
                    instancesList = new ArrayList<>();
                    assignment.put(containerId, instancesList);
                }
                instancesList.add(instancePlanDescription);
            }
        }

        if (lastAssignment == null || !assignmentAreEqual(assignment, lastAssignment)) {
            lastAssignment = assignment;
            if (!lastAssignment.keySet().isEmpty()) {
                String serializedAssignment = serializeAssignment(lastAssignment);
                logger.info("ASSIGNMENT CHANGED!!!");
                logger.info("Last assignment: " + serializedAssignment);
                // invoke DataManager to store assignment to DB.
//                DataManager.getInstance().storeAssignment()
            }
        }
    }

    private boolean assignmentAreEqual(Map<Integer, List<String>> a1, Map<Integer, List<String>> a2) {
        if (a1.keySet().size() != a2.keySet().size())
            return false;

        for (Integer containerId : a1.keySet()) {
            if (!a2.keySet().contains(containerId)) {
                return false;
            }

            List<String> t1 = a1.get(containerId);
            List<String> t2 = a2.get(containerId);

            if (t1.size() != t2.size()) {
                return false;
            }

            for (String instance : t1) {
                if (!t2.contains(instance)) {
                    return false;
                }
            }
        }
        return true;
    }

    private String serializeAssignment(Map<Integer, List<String>> assignment) {
        StringBuilder builder = new StringBuilder();
        for (Integer containerId : assignment.keySet()) {
            List<String> instances = assignment.get(containerId);
            String instanceStr = Utils.collectionToString(instances);
            builder.append("(" + containerId + " : " + instanceStr + ")");
        }
        return builder.toString();
    }
}
