package zyt.custom.my.scheduler.component;

public class Node {

    /**
     * Nodename
     */
    private final String name;

    /**
     * Node cpuload capacity
     */
    private final long capacity;

    /**
     * Cpu core number
     */
    private final int cores;

    /**
     * CPU load of this node
     */
    private long load;

    /**
     * Add for heron
     * unused
     */
    private int totalContainerCount;

    /**
     * Node number in the cluster
     */
    private int nodeCount;

    public Node(String name, long capacity, int cores) {
        this.name = name;
        this.capacity = capacity;
        this.cores = cores;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public long getLoad() {
        return load;
    }

    public void addLoad(long load) {
        this.load += load;
    }

    public String getName() {
        return name;
    }

    public long getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return name + " [cores: " + cores + ", current load: " + load + "/" + capacity + "; ]";
    }


}
