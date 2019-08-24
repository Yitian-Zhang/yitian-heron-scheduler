package zyt.custom.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

public class FileReaderMine {

    private static String txtFile = "/home/yitian/heron-conf/A_Tale_of_Two_City.txt";
    private BufferedReader bufferedReader = null;
    private File file = null;

    public static void main(String[] args) throws Exception {
        FileReaderMine reader = new FileReaderMine();
        while (true) {
            reader.readFile();
        }
    }

    public void readFile() {

        try {
            file = new File(txtFile);
            bufferedReader = new BufferedReader(new java.io.FileReader(file));

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.equals("")) {
                    System.out.println(line);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
