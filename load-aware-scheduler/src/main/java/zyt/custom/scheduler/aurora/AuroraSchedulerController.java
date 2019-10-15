package zyt.custom.scheduler.aurora;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import zyt.custom.scheduler.rescheduler.AuroraRescheduler;
import zyt.custom.scheduler.rescheduler.ReschedulerController;
import zyt.custom.utils.YamlUtils;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * Algorithm implementation for HOT-EDGE and LOAD-AWARE
 *
 * @author yitian
 *
 * 2018-07-03 add for re-schedule topology
 * 2018-07-06 noted:
 *      is not successfully for now
 *      detailing exception at POST about this
 * 2018-07-18 noted:
 *      sovled reschedule core problem
 *
 * TODO: reconstructe this class
 */
public class AuroraSchedulerController extends ReschedulerController {

    private static final Logger LOG = Logger.getLogger(AuroraSchedulerController.class.getName());

    private Config config;
    private Config runtime;

    private AuroraRescheduler auroraRescheduler;

    public AuroraSchedulerController() {
    }

    public AuroraSchedulerController(Config config, Config runtime) {
        this.config = config;
        this.runtime = runtime;

        // TODO: restructure modified 5
        this.auroraRescheduler = (AuroraRescheduler) loadRescheduler();
    }

    /**
     * Using Bridge Pattern to invoke the rescheduling algorithm.
     * TODO: restructure modified 3
     */
    @Override
    public void rescheduling(PackingPlan packingPlan) {
        auroraRescheduler.reschedule(packingPlan);
    }

    /**
     * TODO: restructure modified 4
     * @return
     */
    public Object loadRescheduler() {
        String className = (String) YamlUtils.getInstance().getValueByKey("rescheduling", "algorithm0");
        Class clazz = null;
        try {
            // create instance of AuroraRescheduler
            clazz = Class.forName(className);
            Object obj = clazz.newInstance();

            // invoke the initialize function
            Method method = clazz.getMethod("initialize", Config.class, Config.class);
            method.invoke(obj, config, runtime);
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
