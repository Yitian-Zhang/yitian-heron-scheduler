package zyt.custom.scheduler.monitor;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LatencyMonitorTest {

    @Test
    public void main_test() {
        List<String> taskIdList = new ArrayList<>();
        taskIdList.add("001");
        taskIdList.add("002");
        for (int i = 0; i < 10000; i++) {
            for (String taskId : taskIdList) {
                LatencyMonitor.getInstance().setContent(taskId, i);
            }
        }
    }
}
