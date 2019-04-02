package zyt.custom.test;

import zyt.custom.tools.FileReader;

public class FileReaderTest {

    // "/" : this file will be loacated this project classpath that is resources file in maven project
    private static final String txtFile = "/A_Tale_of_Two_City.txt";
    private static FileReader reader;

    public static void main(String[] args) {
        reader = new FileReader(txtFile);
        while (true) {
            System.out.println(reader.nextLine());
        }
    }
}
