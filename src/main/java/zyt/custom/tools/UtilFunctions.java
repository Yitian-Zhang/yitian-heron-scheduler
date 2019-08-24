package zyt.custom.tools;

import java.util.ArrayList;
import java.util.List;

public class UtilFunctions {

    public static List<Integer> deepCopyIntegerArrayList(List<Integer> list) {
        List<Integer> resultList = new ArrayList<>();
        for (Integer item : list) {
            resultList.add(item);
        }
        return resultList;
    }
}
