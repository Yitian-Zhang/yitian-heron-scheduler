package zyt.custom.test.my.scheduler.aurora;

import zyt.custom.my.scheduler.aurora.AuroraSchedulerThread;

public class AuroraSchedulerThreadTest {

    public static void main(String[] args) {
        AuroraSchedulerThread thread = new AuroraSchedulerThread();
        thread.start();
    }
}
