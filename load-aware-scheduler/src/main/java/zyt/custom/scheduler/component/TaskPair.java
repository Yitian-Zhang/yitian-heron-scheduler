package zyt.custom.scheduler.component;

/**
 * @author yitian
 */
public class TaskPair {

    /**
     * source task id
     */
    private final int sourceTaskId;

    /**
     * destination task id
     */
    private final int destinationTaskId;

    /**
     * return to string formed: [sourceTaskId -> DestinationTaskId]
     */
    private final String toString;

    public TaskPair(int sourceTaskId, int destinationTaskId) {
        this.sourceTaskId = sourceTaskId;
        this.destinationTaskId = destinationTaskId;
        this.toString = "[" + sourceTaskId + " -> " + destinationTaskId + "]";
    }

    public int getSourceTaskId() {
        return sourceTaskId;
    }

    public int getDestinationTaskId() {
        return destinationTaskId;
    }

    @Override
    public String toString() {
        return toString;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + destinationTaskId;
        result = prime * result + sourceTaskId;
        return result;
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
        TaskPair other = (TaskPair) obj;
        if (destinationTaskId != other.destinationTaskId) {
            return false;
        }
        if (sourceTaskId != other.sourceTaskId) {
            return false;
        }
        return true;
    }
}
