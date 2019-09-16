package zyt.custom.tools;

import org.junit.Test;

import static org.junit.Assert.*;

public class FileUtilsTest {

    private String filename = "";

    @Test
    public void liunx_test(String[] args) {
        filename = "/home/yitian/logs/latency/aurora/latency-monitor.txt";
        FileUtils.writeToFile(filename, "content");
        FileUtils.writeToFile(filename, "add this content");

    }

    @Test
    public void windows_test() {
        filename = "C:\\Users\\Administrator\\Desktop\\heron latency\\text.txt";
        FileUtils.writeToFile(filename, "content");
        FileUtils.writeToFile(filename, "add this content");
    }

}