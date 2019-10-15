package zyt.custom.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class YamlUtils {

    private static Map<String, Map<String, Object>> properties;

    private YamlUtils() {
        if (SingletonHolder.instance != null) {
            throw new IllegalStateException();
        }
    }

    public static YamlUtils getInstance() {
        return SingletonHolder.instance;
    }

    //init property when class is loaded
    static {
        InputStream in = null;
        try {
            properties = new HashMap<>();
            Yaml yaml = new Yaml();
            in = YamlUtils.class.getClassLoader().getResourceAsStream("online-rescheduling.yaml");
            properties = yaml.loadAs(in, HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * get yaml property
     *
     * @param key
     * @return
     */
    public Object getValueByKey(String root, String key) {
        Map<String, Object> rootProperty = properties.get(root);
        return rootProperty.getOrDefault(key, "");
    }

    /**
     * use static inner class  achieve singleton
     */
    private static class SingletonHolder {
        private static YamlUtils instance = new YamlUtils();
    }

}
