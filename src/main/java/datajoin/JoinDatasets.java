package datajoin;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class JoinDatasets {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            System.out.println("Required args");
            System.exit(0);
        }

        File file = new File(args[0]);
        Scanner scan = new Scanner(file);

        List<String[]> stations = new ArrayList<>();

        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            if (line.isEmpty()) continue;

            int i = line.indexOf(" ");
            String usaf = line.substring(0, i);

            int pos = line.indexOf("+", i);
            int neg = line.indexOf("-", i);
            int work = -1;
            if (pos >= 0) work = pos;
            if (neg >= 0 && neg < work) work = neg;

            if (work < 0) continue;
            String lat = line.substring(work, line.indexOf(" ", work));

            pos = line.indexOf("+", work + 1);
            neg = line.indexOf("-", work + 1);
            work = -1;
            if (pos >= 0) work = pos;
            if (neg >= 0 && neg < work) work = neg;

            if (work < 0) continue;
            String lon = line.substring(work, line.indexOf(" ", work));

            stations.add(new String[]{usaf, lat, lon});
        }

        stations.forEach(strings -> System.out.println(Arrays.toString(strings)));
    }

}
