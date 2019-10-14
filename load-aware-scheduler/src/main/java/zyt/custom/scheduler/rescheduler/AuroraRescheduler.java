package zyt.custom.scheduler.rescheduler;

import com.twitter.heron.spi.packing.PackingPlan;

/**
 * @author yitian
 */
public interface AuroraRescheduler {

    /**
     * Reschedule function
     * @param packingPlan the current packing plan
     */
    void reschedule(PackingPlan packingPlan);
}
