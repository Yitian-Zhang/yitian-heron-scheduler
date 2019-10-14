package zyt.custom.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class YamlUtilsTest {

    @Test
    public void main_test() {
        String algorithm0 = (String) YamlUtils.getInstance().getValueByKey("rescheduling", "algorithm0");
        String algorithm1 = (String) YamlUtils.getInstance().getValueByKey("rescheduling", "algorithm1");
        String algorithm2 = (String) YamlUtils.getInstance().getValueByKey("rescheduling", "algorithm2");

        System.out.println(algorithm0);
        System.out.println(algorithm1);
        System.out.println(algorithm2);
    }

}