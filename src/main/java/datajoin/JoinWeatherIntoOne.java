package datajoin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class JoinWeatherIntoOne {

    public static void main(String[] args) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));

        Files.find(Paths.get(args[0]), Integer.MAX_VALUE, (path, attr) -> attr.isRegularFile()).forEach(path -> {
            try {
                Scanner scan = new Scanner(path);
                while (scan.hasNextLine()) {
                    bw.write(scan.nextLine() + "\n");
                }
                scan.close();

                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        bw.close();
    }

}
