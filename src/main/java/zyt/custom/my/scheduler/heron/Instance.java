package zyt.custom.my.scheduler.heron;

public class Instance {
    private Integer instanceId;
    private Long workLoad;

    public Instance(Integer instanceId, Long workLoad) {
        this.instanceId = instanceId;
        this.workLoad = workLoad;
    }

    @Override
    public String toString() {
        return "Instance: [instanceId: " + this.instanceId + ", " + this.workLoad + "]";
    }

    public Integer getId() {
        return instanceId;
    }

    public void setId(Integer id) {
        this.instanceId = id;
    }

    public Long getWorkLoad() {
        return workLoad;
    }

    public void setWorkLoad(Long workLoad) {
        this.workLoad = workLoad;
    }
}
