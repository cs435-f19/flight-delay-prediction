package datajoin;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinDatasets {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 2) {
            System.out.println("Required args");
            System.exit(1);
        }

        List<String[]> stations = readStationsFromFile(args[0]);
        List<String[]> airports = readAirportsFromFile(args[1]);

        System.out.println(stations.size());
        System.out.println(airports.size());

        List<String[]> joined = combineAirportsAndStations(stations, airports);
        System.out.println(joined.size());
    }

    private static List<String[]> combineAirportsAndStations(List<String[]> stations, List<String[]> airports) {
        List<String[]> results = new ArrayList<>();

        for (String[] airport : airports) {
            double aLat, aLon;

            try {
                aLat = Double.parseDouble(airport[1]);
                aLon = Double.parseDouble(airport[2]);
            } catch (NumberFormatException e) {
                continue;
            }

            String[] closest = null;
            double dist = Double.MAX_VALUE;

            for (String[] station : stations) {
                double d = gpsDistance(aLat, aLon, Double.parseDouble(station[1]), Double.parseDouble(station[2]));
                if (d < dist) {
                    dist = d;
                    closest = station;
                }
            }

//            System.out.println(Arrays.toString(closest) + "\t" + Arrays.toString(airport) + "\t" + dist);
            results.add(new String[]{airport[0], airport[1], airport[2], closest[0], closest[1], closest[2]});
        }

        return results;
    }

    private static double gpsDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = haversin(dLat) + Math.cos(lat1) * Math.cos(lat2) * haversin(dLon);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return 6371 * c;
    }

    private static double haversin(double val) {
        return Math.sin(val / 2) * Math.sin(val / 2);
    }

    private static List<String[]> readAirportsFromFile(String file) throws FileNotFoundException {
        List<String[]> airports = new ArrayList<>();

        Scanner scan = new Scanner(new File(file));
        scan.nextLine();
        scan.nextLine();

        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            if (line.isEmpty()) continue;

            String[] split = line.split(",");

            String iata = split[13];
            if (iata == null || iata.isEmpty()) continue;

//            System.out.println(iata + "\t" + split[4] + "\t" + split[5]);
            airports.add(new String[]{iata, split[4], split[5]});
        }

        return airports;
    }

    private static List<String[]> readStationsFromFile(String arg) throws FileNotFoundException {
        List<String[]> stations = new ArrayList<>();

        Scanner scan = new Scanner(new File(arg));

        Pattern regex = Pattern.compile("\\s[-+][0-9]+\\.[0-9]+");

        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            if (line.isEmpty()) continue;

            String usaf = line.substring(0, line.indexOf(" "));

            Matcher matcher = regex.matcher(line);

            if (!matcher.find()) continue;
            String lat = line.substring(matcher.start() + 1, matcher.end());

            if (!matcher.find()) continue;
            String lon = line.substring(matcher.start() + 1, matcher.end());

//            int pos = line.indexOf("+", i);
//            int neg = line.indexOf("-", i);
//            int work = -1;
//            if (pos >= 0) work = pos;
//            if (neg >= 0 && neg < work) work = neg;
//
//            if (work < 0) continue;
//            String lat = line.substring(work, line.indexOf(" ", work));
//
//            pos = line.indexOf("+", work + 1);
//            neg = line.indexOf("-", work + 1);
//            work = -1;
//            if (pos >= 0) work = pos;
//            if (neg >= 0 && neg < work) work = neg;
//
//            if (work < 0) continue;
//            String lon = line.substring(work, line.indexOf(" ", work));

            stations.add(new String[]{usaf, lat, lon});
        }
        return stations;
    }

}
