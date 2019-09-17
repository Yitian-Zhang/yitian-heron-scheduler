package zyt.custom.scheduler;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.log4j.Logger;
import zyt.custom.cpuinfo.CPUInfo;
import zyt.custom.scheduler.component.Executor;
import zyt.custom.scheduler.component.ExecutorPair;
import zyt.custom.scheduler.component.Node;
import zyt.custom.scheduler.component.Instance;
import zyt.custom.tools.Utils;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataManager {

    private Logger logger = Logger.getLogger(DataManager.class);

    /**
     * 单例模式
     */
    private static DataManager instance = null;

    private DataSource dataSource;

    /**
     * host name set in CONFIG_FILE
     */
    private String nodeName;

    /**
     * percentage of cpu load: default = 60
     */
    private int capacity = 60;


    private DataManager() {
        try {
            // Load DB configuration from CONFIG_FILE
            logger.debug("Loading Heron cluster configuration from file: " + Constants.DATABASE_CONFIG_FILE);
            Properties clusterProperties = new Properties();
            clusterProperties.load(new FileInputStream(Constants.DATABASE_CONFIG_FILE));

            nodeName = clusterProperties.getProperty("node-name");
            if (clusterProperties.getProperty("capacity") != null) {
                capacity = Integer.parseInt(clusterProperties.getProperty("capacity"));
                if (capacity < 1 || capacity > 100) {
                    throw new RuntimeException("Wrong capacity: " + capacity + ", expected in the range [1, 100]");
                }
            }

            // Set DB connection pool
            logger.debug("Loading configuration from file: " + Constants.DBCP_CONFIG_FILE);
            Properties properties = new Properties();
            properties.load(new FileInputStream(Constants.DBCP_CONFIG_FILE));

            // Setting up pooling data source
            dataSource = BasicDataSourceFactory.createDataSource(properties);

            logger.info("DataManager is started.");
        } catch (Exception e) {
            logger.error("Error starting DataManager: ", e);
        }
    }

    public static synchronized DataManager getInstance() {
        if (instance == null)
            instance = new DataManager();
        return instance;
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Test function
     *
     * @throws SQLException
     */
    public void connectionTest() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        logger.debug("Start mysql connection test.");
        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "select id from tb_test";
            logger.debug("SQL Script: " + sql);

            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                logger.debug("Query result: " + resultSet.getString("id"));
            }
        } catch (SQLException e) {
            logger.error("Test connection is wrong: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Store the CPU load of each task in every worker nodes
     * The beginTask and endTask in a Executor is the same value
     *
     * @param topologyId topology id
     * @param beginTask task id
     * @param endTask task id
     * @param load CPU load
     * @throws SQLException
     */
    public void storeCpuLoad(String topologyId, int beginTask, int endTask, long load) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        logger.debug("Storing load stat (topology: " + topologyId
                + ", executor: [" + beginTask + ", " + endTask + "], load: " + load + " CPU cycles per second)");
        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "update `tb_cpu_load` set `load` = " + load + ", node = '" + nodeName
                    + "' where topology_id = '" + topologyId
                    + "' and begin_task = " + beginTask
                    + " and end_task = " + endTask;
            if (statement.executeUpdate(sql) == 0) {
                sql = "insert into `tb_cpu_load`(topology_id, begin_task, end_task, `load`, node) " +
                        "values('" + topologyId + "', " + beginTask + ", " + endTask + ", " + load + ", '" + nodeName + "')";
                statement.executeUpdate(sql);
            }
        } catch (SQLException e) {
            logger.error("Storing load stat, error: ", e);
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Store traffic of a pair of task to DB
     *
     * @param topologyId      topologyId
     * @param sourceTask      sourceTask Id
     * @param destinationTask destination task id
     * @param traffic         sourceTask -> destinationTask
     * @throws SQLException
     */
    public void storeTraffic(String topologyId, int sourceTask, int destinationTask, int traffic) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        logger.debug("Storing traffic stat (topology: " + topologyId + ", sourceTask: "
                + sourceTask + ", destination: " + destinationTask + ", traffic: " + traffic + " tuples per second)");
        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "update tb_traffic set traffic = " + traffic
                    + " where topology_id = '" + topologyId
                    + "' and source_task = " + sourceTask
                    + " and destination_task = " + destinationTask;
            if (statement.executeUpdate(sql) == 0) {
                sql = "insert into tb_traffic(topology_id, source_task, destination_task, traffic) " +
                        "values('" + topologyId + "', " + sourceTask + ", " + destinationTask + ", " + traffic + ")";
                statement.executeUpdate(sql);
            }
        } catch (SQLException e) {
            logger.error("Storing traffic, error: ", e);
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Update or insert Node cpu load
     *
     * @param totalSpeed speed of CPU
     * @throws SQLException
     */
    public void checkNode(long totalSpeed) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();

            long absoluteCapacity = totalSpeed / 100 * capacity;
            String sql = "update tb_node set capacity = " + absoluteCapacity
                    + " where name = '" + nodeName + "'";
            if (statement.executeUpdate(sql) == 0) {
                sql = "insert into tb_node(name, capacity, cores) values('"
                        + nodeName + "', " + totalSpeed + ", " + CPUInfo.getInstance().getNumberOfCores() + ")";
                statement.execute(sql);
            }
        } catch (SQLException e) {
            logger.error("Checking node, error: ", e);
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Get topology total load
     *
     * @param topologyId topology id
     * @return total load of this topology
     */
    public long getTotalLoad(String topologyId) throws Exception {
        logger.debug("Getting total load of topology: " + topologyId);

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        long totalLoad = -1;

        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "select sum(`tb_cpu_load`.`load`) from `tb_cpu_load` where topology_id = '" + topologyId + "'";
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                totalLoad = resultSet.getLong(1);
            } else {
                throw new Exception("Cannot find topology " + topologyId + " in the DB.");
            }
        } catch (SQLException e) {
            logger.error("Getting total load for topology: " + topologyId + ", error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        logger.info("Total load for topology " + topologyId + ": " + totalLoad + " Hz/s. ");
        return totalLoad;
    }

    /**
     * Get worker node info
     *
     * @return nodeName -> Node info
     * @throws SQLException
     */
    public Map<String, Node> getNodes() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        Map<String, Node> nodeMap = new HashMap<>();

        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "select name, capacity, cores from tb_node";
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String name = resultSet.getString(1);
                long capacity = resultSet.getLong(2);
                int cores = resultSet.getInt(3);
                nodeMap.put(name, new Node(name, capacity, cores));
            }
            int nodeCount = nodeMap.keySet().size();
            for (Node node : nodeMap.values()) {
                node.setNodeCount(nodeCount);
            }

        } catch (SQLException e) {
            logger.error("Getting nodes, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return nodeMap;
    }

    /**
     * Get traffic between executor from begin_task -> end_task
     *
     * @param topologyId topology id
     * @return List of ExecutorPair
     */
    public List<ExecutorPair> getInterExecutorTrafficList(String topologyId) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<ExecutorPair> trafficStat = new ArrayList<>();

        try {
            connection = getConnection();
            statement = connection.createStatement();

            // load executors
            List<Executor> executorList = new ArrayList<Executor>();
            String sql = "select begin_task, end_task, `load` from `tb_cpu_load` where topology_id = '" + topologyId + "'";
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                Executor executor = new Executor(resultSet.getInt(1), resultSet.getInt(2));
                executor.setLoad(resultSet.getLong(3));
                executor.setTopologyId(topologyId);
                executorList.add(executor);
            }
            resultSet.close();
            logger.debug("Executor list for topology: " + topologyId + ", includes: "
                    + Utils.collectionToString(executorList));

            sql = "select source_task, destination_task, traffic from tb_traffic order by traffic desc";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                int sourceTask = resultSet.getInt(1); // begin_task
                int destinationTask = resultSet.getInt(2); // destination_task
                int traffic = resultSet.getInt(3); // traffic

                Executor source = Utils.getExecutor(sourceTask, executorList);
                logger.debug("Source executor for source task " + sourceTask + ": " + source);
                Executor destination = Utils.getExecutor(destinationTask, executorList);
                logger.debug("Destination executor for destination task " + destinationTask + ": " + destination);

                if (source != null && destination != null) {
                    ExecutorPair pair = null;
                    for (ExecutorPair tmp : trafficStat) {
                        if (tmp.getSource().equals(source) && tmp.getDestination().equals(destination)) {
                            pair = tmp;
                            break;
                        }
                    }
                    if (pair == null) {
                        pair = new ExecutorPair(source, destination);
                        trafficStat.add(pair);
                    }
                    pair.addTraffic(traffic);
                    // ??
                    int index = trafficStat.indexOf(pair);
                    while (index > 0 && pair.getTraffic() > trafficStat.get(index - 1).getTraffic()) {
                        ExecutorPair executorPair = trafficStat.remove(index - 1);
                        trafficStat.add(index, executorPair);
                        index--;
                    }
                } else {
                    trafficStat.clear();
                    break;
                }

            }
        } catch (SQLException e) {
            logger.error("Retrieving traffic stats for topology: " + topologyId + ", error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return trafficStat;
    }

    /**
     * Get overload nodes list
     * TODO: There is a problem when tb_node table have same heron01 data.
     * Therefore, data in DB should be cleared before each experiment.
     *
     * @return list of node
     */
    public List<Node> getOverloadedNodes() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<Node> nodeList = new ArrayList<>();

        try {
            connection = getConnection();
            statement = connection.createStatement();
            String sql = "select `tb_cpu_load`.node, sum(`load`) as total_load, tb_node.capacity, tb_node.cores " +
                            "from `tb_cpu_load` join tb_node on `tb_cpu_load`.node = tb_node.name " +
                            "group by tb_node.name " +
                            "having total_load > tb_node.capacity";
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String name = resultSet.getString(1);
                long load = resultSet.getLong(2);
                long capacity = resultSet.getLong(3);
                int cores = resultSet.getInt(4);
                Node node = new Node(name, capacity, cores);
                node.addLoad(load);
                nodeList.add(node);
            }
        } catch (SQLException e) {
            logger.error("Getting overloaded nodes, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return nodeList;
    }

    /**
     * Get current inter-node traffic
     *
     * @return current inter-node traffic (int)
     * @throws SQLException
     */
    public int getCurrentInterNodeTraffic() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        int currentInterNodeTraffic = 0;

        try {
            connection = getConnection();
            statement = connection.createStatement();

            // load executors
            List<Executor> executorList = new ArrayList<>();
            String sql = "select begin_task, end_task, `load`, node from `tb_cpu_load`";
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                Executor executor = new Executor(resultSet.getInt(1), resultSet.getInt(2));
                executor.setLoad(resultSet.getLong(3));
                executor.setNode(resultSet.getString(4));
                executorList.add(executor);
            }
            resultSet.close();
            // For debug:
//            logger.debug("Executor list: " + Utils.collectionToString(executorList));

            // load tasks and create the list the executor pairs sorted by traffic desc
            sql = "select source_task, destination_task, traffic from tb_traffic";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                // load data from DB
                int sourceTask = resultSet.getInt(1);
                int destinationTask = resultSet.getInt(2);
                int traffic = resultSet.getInt(3);

                // look up executor pair
                Executor source = Utils.getExecutor(sourceTask, executorList);
                logger.debug("source executor for source task: " + sourceTask + ": " + source);
                Executor destination = Utils.getExecutor(destinationTask, executorList);
                logger.debug("destination executor for destination task: " + destinationTask + ": " + destination);

                if (source != null && destination != null && !source.getNode().equals(destination.getNode())) {
                    logger.debug("Tasks " + sourceTask + " and " + destinationTask +
                            " are currently deployed on distinct nodes, so they contribute to inter-node traffic for "
                            + traffic + " tuple/s");
                    currentInterNodeTraffic += traffic;
                }
            }
        } catch (SQLException e) {
            logger.error("Computing current inter-node traffic, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return currentInterNodeTraffic;
    }

    /**
     * calculate hot edges in topology
     * Update: 2018-07-01
     * add sort pair with traffic desc
     *
     * @throws SQLException
     */
    public List<ExecutorPair> calculateHotEdges(String topologyId) throws SQLException {
        logger.debug("Getting hot edge from DB using task load, for topology: " + topologyId);

        // calculate average
        List<ExecutorPair> trafficList = DataManager.getInstance().getInterExecutorTrafficList(topologyId);
        double totalTraffic = 0;
        for (ExecutorPair pair : trafficList) {
            totalTraffic += pair.getTraffic();
        }
        // calculate average traffic
        double averageTraffic = totalTraffic / trafficList.size();

        // calculate variance
        double variance = 0;
        for (ExecutorPair pair : trafficList) {
            variance += (pair.getTraffic() - averageTraffic) * (pair.getTraffic() - averageTraffic);
        }

        // calculate standard deviation
        /*
         Output example:
            Total traffic is: 84592.0, average traffic between executor is: 21148.0
            Variance is: 7.4148074E7 and standard deviation is: 4305.463796154835
         */
        double standardDeviation = Math.sqrt(variance / trafficList.size());
        System.out.println("Total traffic is: " + totalTraffic + ", average traffic between executor is: " + averageTraffic);
        System.out.println("Variance is: " + variance + " and standard deviation is: " + standardDeviation);

        double totalBias = 0;
        for (ExecutorPair pair : trafficList) {
            /*
             update: 20180604
             double bias = pair.getTraffic() - averageTraffic;
             */
            double bias = Math.abs(pair.getTraffic() - averageTraffic);
            totalBias += bias;
        }
        double averageBias = totalBias / trafficList.size();

        List<ExecutorPair> hostPairsList = new ArrayList<>();
        for (ExecutorPair pair : trafficList) {
            double bias = Math.abs(pair.getTraffic() - averageTraffic);
            logger.info("Pair traffic: " + pair.getTraffic() + " average traffic: " + averageTraffic + " bias: "
                    + bias + " average bias: " + averageBias);
            if (pair.getTraffic() > averageTraffic && bias > averageBias) {
                logger.debug("Hot edge: " + pair);
                hostPairsList.add(pair);
            } else if (pair.getTraffic() > averageTraffic && bias < averageBias) { // 2018-05-22 add
                logger.debug("Medium hot edge: " + pair);
                hostPairsList.add(pair);
            } else if (pair.getTraffic() < averageTraffic && bias < averageBias) {
                logger.debug("Medium cool edge: " + pair);
            } else if (pair.getTraffic() < averageTraffic && bias > averageBias) {
                logger.debug("Cool edge: " + pair);
            }
        }
        logger.info(Utils.collectionToString(hostPairsList));
        return hostPairsList;
    }

    /**
     * For hot edge scheduling
     *
     * @return
     * @throws SQLException
     */
    public Map<Integer, Long> getTaskLoadList() throws SQLException {
        Map<Integer, Long> taskLoadMap = new HashMap<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "select `begin_task`, `load` from `tb_cpu_load` order by `load` desc";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                int taskId = resultSet.getInt(1);
                long load = resultSet.getLong(2);
                taskLoadMap.put(taskId, load);
            }

        } catch (SQLException e) {
            logger.error("Getting task load, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return taskLoadMap;
    }

    /**
     * Get cpu load of worker node
     *
     * @return
     * @throws SQLException
     */
    public Map<String, String> getCpuUsageOfHost() throws SQLException {
        Map<String, String> cpuUsageOfHost = new HashMap<>();
        Map<String, Node> hostList = DataManager.getInstance().getNodes();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            statement = connection.createStatement();

            for (String hostname : hostList.keySet()) {
                String sql = "select sum(`load`) from `tb_cpu_load` where node='" + hostname + "'";
                resultSet = statement.executeQuery(sql);
                if (resultSet.next()) {
                    long hostCpuLoad = resultSet.getLong(1);
                    double hostCpuTotal = hostList.get(hostname).getCapacity() / 0.4;
                    double hostCpuUsage = hostCpuLoad / hostCpuTotal;
                    logger.debug("Hostname: " + hostname + ", total load: " + hostCpuTotal + ", cpu load: "
                            + hostCpuLoad + ", Usage: " + hostCpuUsage);
                    cpuUsageOfHost.put(hostname, Double.toString(hostCpuUsage));
                }
            }

        } catch (SQLException e) {
            logger.error("Getting cpu load of host, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return cpuUsageOfHost;
    }

    /**
     * Get all instance list by DESC
     *
     * @return
     * @throws SQLException
     */
    public List<Instance> getInstanceListByDesc() throws SQLException {
        List<Instance> instanceList = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql = "select `begin_task`, `load` from `tb_cpu_load` order by `load` desc";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                int taskId = resultSet.getInt(1);
                long load = resultSet.getLong(2);
                Instance instance = new Instance(taskId, load);
                instanceList.add(instance);

            }
            logger.debug("Instance list is: " + Utils.collectionToString(instanceList));
        } catch (SQLException e) {
            logger.error("Getting instance list, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return instanceList;
    }

    /**
     * Get node cpu load info
     *
     * @return
     * @throws SQLException
     */
    public List<Node> getLoadOfNode() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<Node> nodeList = new ArrayList<>();

        try {
            connection = getConnection();
            statement = connection.createStatement();

            String sql =
                    "select `tb_cpu_load`.node, sum(`load`) as total_load, tb_node.capacity, tb_node.cores " +
                            "from `tb_cpu_load` join tb_node on `tb_cpu_load`.node = tb_node.name " +
                            "group by tb_node.name ";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String name = resultSet.getString(1);
                long load = resultSet.getLong(2);
                long capacity = resultSet.getLong(3);
                int cores = resultSet.getInt(4);
                Node node = new Node(name, capacity, cores);
                node.addLoad(load);
                nodeList.add(node);
            }
        } catch (SQLException e) {
            logger.error("Getting load of nodes, error: ", e);
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return nodeList;
    }

    /**
     * Calculate the load difference of worker nods in the cluster
     * as the condition for trigger rescheduling
     *
     * @param nodeList list of node
     * @return
     */
    public int calculateDifferentLoadForTrigger(List<Node> nodeList) {
        long maxLoad = Long.MIN_VALUE;
        String maxLoadName = "";
        for (Node node : nodeList) {
            long load = node.getLoad();
            if (load > maxLoad) {
                maxLoad = load;
                maxLoadName = node.getName();
            }
        }
        long minLoad = Long.MAX_VALUE;
        String minLoadName = "";
        for (Node node : nodeList) {
            long load = node.getLoad();
            if (load < minLoad) {
                minLoad = load;
                minLoadName = node.getName();
            }
        }
        int differentPercentage = (int) ((maxLoad - minLoad) * 100 / maxLoad);
        logger.info("Node with max load is: [" + maxLoadName + ", " + maxLoad + "]."
                + "Node with min load is: [" + minLoadName + ", " + minLoad + "]."
                + "The difference percentage of the two nodes is: " + differentPercentage);
        return differentPercentage;
    }
}
