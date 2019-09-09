package zyt.custom.my.scheduler.component;

public class ExecutorPair {

    /**
     * Start instance (include one task)
     */
    private final Executor source;

    /**
     * End instance (include one task)
     */
    private final Executor destination;

    /**
     * The traffic between the source and destination task
     */
    private int traffic;

    public ExecutorPair(Executor source, Executor destination) {
        this.source = source;
        this.destination = destination;
    }

    public static long getKey(Executor source, Executor destination) {
        return source.getBeginTask() << 32 + destination.getBeginTask();
    }

    public Executor getSource() {
        return source;
    }

    public Executor getDestination() {
        return destination;
    }

    public int getTraffic() {
        return traffic;
    }

    public void addTraffic(int traffic) {
        this.traffic += traffic;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((destination == null) ? 0 : destination.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
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
        ExecutorPair other = (ExecutorPair) obj;
        if (destination == null) {
            if (other.destination != null) {
                return false;
            }
        } else if (!destination.equals(other.destination)) {
            return false;
        }
        if (source == null) {
            if (other.source != null) {
                return false;
            }
        } else if (!source.equals(other.source)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        //##20181108  simple output in LOG
//		return "{" + source + " -> " + destination + ": " + traffic + " tuple/s}";
        return "{" + source.getBeginTask() + " -> " + destination.getBeginTask() + ": " + traffic + " tuple/s}";
    }
}
