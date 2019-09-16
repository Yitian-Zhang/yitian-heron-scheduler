package zyt.custom.scheduler;

import org.junit.Test;
import zyt.custom.scheduler.component.ExecutorPair;
import zyt.custom.scheduler.component.Node;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class DataManagerTest {

    private String topologyId = "";

    @Test
    public void base_test() throws Exception {
        topologyId = "";
        DataManager.getInstance().connectionTest(); // success
        DataManager.getInstance().storeCpuLoad("001", 2, 3, 111l); // success
        DataManager.getInstance().storeTraffic("001", 1, 2, 5222); // success
        DataManager.getInstance().checkNode(7843947l); // success
        DataManager.getInstance().getTotalLoad(topologyId); // success

    }

    @Test
    public void getNodes_test() throws SQLException {
        topologyId = "BenchmarkSentenceWordCountTopologyd72570cf-4fcf-469c-b00d-f1a0056b81de";
        Map<String, Node> nodeMap = DataManager.getInstance().getNodes();
        for (String name : nodeMap.keySet()) {
            System.out.println(nodeMap.get(name)); // heron01 [cores: 4, current load: 0/4532364900; ]
        }
    }

    @Test
    public void getInterExecutorTrafficList_test() throws SQLException {
        topologyId = "";
        List<ExecutorPair> trafficList = DataManager.getInstance().getInterExecutorTrafficList(topologyId);
        for (ExecutorPair pair : trafficList) {
            System.out.println(pair);
        }
    }

    @Test
    public void getOverloadedNodes_test() throws SQLException {
        List<Node> nodeList = DataManager.getInstance().getOverloadedNodes();
        for (Node node : nodeList) {
            System.out.println(node);
        }
    }

    @Test
    public void getCurrentInterNodeTraffic_test() throws SQLException {
        int interNodeTraffic = DataManager.getInstance().getCurrentInterNodeTraffic();
        System.out.println("interNodeTraffic: " + interNodeTraffic);
    }

    @Test
    public void calculateHotEdges_test() throws SQLException {
        topologyId = "";
        DataManager.getInstance().calculateHotEdges(topologyId);

    }

    @Test
    public void getTaskLoadList_test() throws SQLException {
        Map<Integer, Long> taskLoadMap = DataManager.getInstance().getTaskLoadList();
        for (int taskId : taskLoadMap.keySet()) {
            System.out.println("Task: " + taskId);
        }
    }

    @Test
    public void getCpuUsageOfHost_test() throws SQLException {
        DataManager.getInstance().getCpuUsageOfHost();
    }

    @Test
    public void getLoadOfNode_test() throws SQLException {
        List<Node> overloadNodeList = DataManager.getInstance().getLoadOfNode();
        for (Node node : overloadNodeList) {
            System.out.println(node);
        }
        int differentPercentage = DataManager.getInstance().calculateDifferentLoadForTrigger(overloadNodeList);
        System.out.println("The different percentage is: " + differentPercentage);
    }
}
