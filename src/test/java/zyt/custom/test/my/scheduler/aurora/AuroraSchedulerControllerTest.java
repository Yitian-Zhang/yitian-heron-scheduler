package zyt.custom.test.my.scheduler.aurora;

import zyt.custom.tools.UtilFunctions;

import java.util.ArrayList;
import java.util.List;

public class AuroraSchedulerControllerTest {


    public static void main(String[] args) {
        List<Integer> originalList = new ArrayList<>();
        originalList.add(1);
        originalList.add(2);
        originalList.add(3);
        originalList.add(4);

        List<Integer> newList = UtilFunctions.deepCopyIntegerArrayList(originalList);
        removeItemInFunction(newList);
        showList(newList);
        System.out.println("...");
        showList(originalList);
    }

    private static void removeItemInFunction(List<Integer> originalList) {
        Integer item = new Integer(2);
        originalList.remove(item);
    }

    private static void showList(List<Integer> list) {
        for (int item : list) {
            System.out.println(item);
        }
    }
}
