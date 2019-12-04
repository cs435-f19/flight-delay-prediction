package datajoin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class OutputCombiner {

    public static void main(String[] args) throws IOException {
        List<Path> paths = new ArrayList<>();

        Files.find(Paths.get(args[0]), 1, (path, attr) -> attr.isRegularFile()).forEach(paths::add);

        boolean wroteHeader = false;
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
        for (Path path : paths) {
            System.out.println(path);

            Scanner scan = new Scanner(path);
            String header = scan.nextLine();
            if (!wroteHeader) {
                bw.write(header + "\n");
                wroteHeader = true;
            }
            while (scan.hasNextLine()) {
                bw.write(scan.nextLine() + "\n");
            }
            scan.close();
            bw.flush();
        }
        bw.close();
    }

}
