package zyt.custom.scheduler.aurora;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author yitian
 */
public class AuroraSchedulerControllerTest {

    @Test
    public void loadRescheduler() {
        AuroraSchedulerController controller = new AuroraSchedulerController();
        Object obj = controller.loadRescheduler();
        System.out.println(obj.getClass().getName());
    }
}