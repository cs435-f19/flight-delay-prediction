package datajoin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class WeatherFileFinder {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Arguments:   WEATHER_FOLDER AIRPORT_STATIONS_CSV TARGET_FOLDER");

            System.exit(1);
        }

        Set<String> stations = new HashSet<>();
        Scanner scan = new Scanner(new File(args[1]));
        scan.nextLine(); // Skip labels line
        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            stations.add(line.split(",")[3]);
        }
        scan.close();

        Files.find(Paths.get(args[0]), Integer.MAX_VALUE, (path, attr) -> {
            String s = path.getFileName().toString();
            return attr.isRegularFile() && s.contains(".") && stations.contains(s.substring(0, s.indexOf(".")));
        }).forEach(path -> {
            try {
                Files.copy(path, Paths.get(args[2]).resolve(path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


    }

}
