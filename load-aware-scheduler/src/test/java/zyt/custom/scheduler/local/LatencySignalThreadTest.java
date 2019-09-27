package zyt.custom.scheduler.local;

import org.junit.Test;

import static org.junit.Assert.*;

public class LatencySignalThreadTest {

    @Test
    public void main_test() {
        // no synchronized test
        System.out.println("Latency Monitor started ...");
        LatencySignalThread monitor1 = new LatencySignalThread();
        LatencySignalThread monitor2 = new LatencySignalThread();
        monitor1.start(); // 启动线程
        monitor2.start();

        int i = 0;
        while (true) {
            monitor1.setContent(i + "", i);
            monitor2.setContent(i + "", i);
            i++;
        }
    }

}