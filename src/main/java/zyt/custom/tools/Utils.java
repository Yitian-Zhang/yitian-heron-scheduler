package zyt.custom.tools;

import zyt.custom.my.scheduler.component.Executor;

import java.util.Collection;
import java.util.List;

public class Utils {

    private Utils() {
    }

    /**
     * convert collection to string for printing
     * @param list
     * @return the list in csv format
     */
    public static String collectionToString(Collection<?> list) {
        if (list == null) {
            return "null";
        }
        if (list.isEmpty()) {
            return "<empty list>";
        }
        StringBuffer stringBuffer = new StringBuffer();
        int i = 0;
        for (Object item : list) {
            stringBuffer.append(item.toString());
            if (i < list.size() - 1) {
                stringBuffer.append(", ");
            }
            i++;
        }
        return stringBuffer.toString();
    }

    public static Executor getExecutor(int taskId, List<Executor> executorList) {
        for (Executor executor : executorList) {
            if (executor.getBeginTask() == taskId) {
                return executor;
            }
        }
        return null;
    }
}
