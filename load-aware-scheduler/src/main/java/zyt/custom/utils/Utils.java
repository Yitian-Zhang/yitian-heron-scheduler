package zyt.custom.utils;

import zyt.custom.scheduler.component.Executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Utils {

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

    public static List<Integer> deepCopyIntegerArrayList(List<Integer> list) {
        List<Integer> resultList = new ArrayList<>();
        for (Integer item : list) {
            resultList.add(item);
        }
        return resultList;
    }
}
