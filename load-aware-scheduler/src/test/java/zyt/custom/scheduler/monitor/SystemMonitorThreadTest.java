package zyt.custom.scheduler.monitor;

import org.junit.Test;

import static org.junit.Assert.*;

public class SystemMonitorThreadTest {

    @Test
    public void main_test() {
        new SystemMonitorThread().start();
    }

}