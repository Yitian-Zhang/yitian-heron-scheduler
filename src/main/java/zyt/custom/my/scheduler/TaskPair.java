package zyt.custom.my.scheduler;

public class TaskPair {
    private final int sourceTaskId; // source task id
    private final int destinationTaskId; // destination task id
    private final String toString; // return to string formed: [sourceTaskId -> DestinationTaskId]

    /**
     * Constructor
     * @param sourceTaskId
     * @param destinationTaskId
     */
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

    /**
     * Get hash code
     * @return
     */
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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TaskPair other = (TaskPair) obj;
        if (destinationTaskId != other.destinationTaskId)
            return false;
        if (sourceTaskId != other.sourceTaskId)
            return false;
        return true;
    }
}
