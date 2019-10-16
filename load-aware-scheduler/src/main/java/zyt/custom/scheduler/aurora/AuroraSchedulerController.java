package zyt.custom.scheduler.aurora;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;
import zyt.custom.scheduler.rescheduler.AuroraRescheduler;
import zyt.custom.scheduler.rescheduler.ReschedulerController;
import zyt.custom.utils.YamlUtils;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * This class extends ReschedulerController that has only one function named "rescheduling".
 * In the rescheduling, it invokes the "reschedule" function defined in AuroraScheduler interface.
 *
 * AuroraScheduler is a interface that declare a function named "reschedule".
 * And, ExampleRescheduler, DataStreamCateRescheduler and LoadAwareRescheduler implemented AuroraScheduler.
 * They defined the specific function content of the reschedule to implement your own rescheduling algorithm.
 *
 * AuroraSchedulerController, ReschedulerController, AuroraRescheduler and ExampleRescheduler (DataStreamCate/LoadAware)
 * build the Bridge Pattern to implement the flexible structure of Heron rescheduling.
 *
 *
 *
 * Processing of this file:
 * 2018-07-03:
 *      add for re-schedule topology.
 * 2018-07-06:
 *      running not successfully for now, detailing exception at POST about this.
 * 2018-07-18:
 *      sovled reschedule core problem
 * 2019-10-15:
 *      restructured this class
 *
 * @author yitian
 */
public class AuroraSchedulerController extends ReschedulerController {

    private static final Logger logger= Logger.getLogger(AuroraSchedulerController.class.getName());

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
     * Reading the full path of a rescheduler class configured in "online-rescheduling.yaml".
     * Build the instance and invoke the "initialize" function to init variables of the class.
     * TODO: restructure modified 4
     * @return
     */
    public Object loadRescheduler() {
        String className = (String) YamlUtils.getInstance().getValueByKey("rescheduling", "active");
        Class clazz = null;
        try {
            // create instance of AuroraRescheduler
            clazz = Class.forName(className);
            logger.info("Loaded the rescheduler class is: " + clazz.getName());

            // invoke the initialize function
            Object obj = clazz.newInstance();
            Method method = clazz.getMethod("initialize", Config.class, Config.class);
            method.invoke(obj, config, runtime);
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
