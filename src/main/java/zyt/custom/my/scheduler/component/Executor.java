package zyt.custom.my.scheduler.component;

/**
 * Executor -> Heron Instance
 * One executor include one task, so the beginTask equals the endTask.
 */
public class Executor {

    private int beginTask;

    private int endTask;

    private long load;

    /**
     * the ececutor in which work node
     */
    private String node;

    /**
     * the executor in which topology
     */
    private String topologyId;

    public Executor() {
        this(-1, -1);
    }

    public Executor(int beginTask, int endTask) {
        this.beginTask = beginTask;
        this.endTask = endTask;
    }

    public boolean match() {
        return false;
    }

    public boolean includes(int task) {
        return task >= beginTask && task <= endTask;
    }

    public void add(int task) {
        if (beginTask == -1 && endTask == -1) {
            beginTask = task;
            endTask = task;
        } else if (task < beginTask) {
            beginTask = task;
        } else if (task > endTask) {
            endTask = task;
        }
    }

    @Override
    public String toString() {
        return "[" + beginTask + ", " + endTask + "]; load: " + load + " Hz/s" + ((node != null) ? "; node: " + node : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Executor other = (Executor) obj;
        if (beginTask != other.beginTask) {
            return false;
        }
        if (endTask != other.endTask) {
            return false;
        }
        return true;
    }

    public int getBeginTask() {
        return beginTask;
    }

    public int getEndTask() {
        return endTask;
    }

    public long getLoad() {
        return load;
    }

    public void setLoad(long load) {
        this.load = load;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }
}
