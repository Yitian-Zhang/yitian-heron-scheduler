package zyt.custom.my.scheduler;

public class Node {
    private final String name;
    private final long capacity;
    private final int cores;
    private long load;
    // add for heron
    private int totalContainerCount;
//    private List<Container> containerToNode;

    // origin for storm
    private int nodeCount;
//    private int totalSlotCount; // this node has assigned how many slot
//    private int availableSlotCount; // how many available slot in this node
//    private Map<Integer, Slot> slotMap; // slot.hashcode() -> slot
//    private int nodeCount; // the number of nodes in this cluster

    public Node(String name, long capacity, int cores) {
        this.name = name;
        this.capacity = capacity;
        this.cores = cores;
//        slotMap = new HashMap<Integer, Slot>();
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
