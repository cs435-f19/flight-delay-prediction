package datajoin;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinAirportsStations {

    private static final String LINE_SEP = System.lineSeparator();


    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Required args:   STATIONS_FILE AIRPORTS_FILE OUTPUT_FILE");
            System.out.println("Output is CSV of the form:\n" +
                    "\tAirport_Callsign,Airport_Lat,Airport_Long,Weather_Station_USAF");
            System.exit(1);
        }

        long t = System.currentTimeMillis();
        List<String[]> stations = readStationsFromFile(args[0]);
        List<String[]> airports = readAirportsFromFile(args[1]);

        System.out.println("Stations: " + stations.size());
        System.out.println("Airports: " + airports.size());

        List<String[]> joined = combineAirportsAndStations(stations, airports);
        System.out.println("Airports with Stations: " + joined.size());

        writeCombinedToCSV(joined, args[2], new String[]{"airport", "a_lat", "a_lon", "station", "s_lat", "s_lon", "station_dist"});

        System.out.println((System.currentTimeMillis() - t) / 1000.0 + " seconds");
    }

    private static void writeCombinedToCSV(List<String[]> joined, String path, String[] headers) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));

        bw.write(String.join(",", headers) + LINE_SEP);

        for (String[] data : joined) {
            bw.write( String.join(",", data) + LINE_SEP);
        }

        bw.flush();
        bw.close();
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

            if (closest == null) continue;

//            System.out.println(Arrays.toString(closest) + "\t" + Arrays.toString(airport) + "\t" + dist);
            results.add(new String[]{airport[0], airport[1], airport[2], closest[0], closest[1], closest[2], dist + ""});
        }

        return results;
    }

    private static double gpsDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return 6371 * c;
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

        scan.close();

        return airports;
    }

    private static List<String[]> readStationsFromFile(String arg) throws FileNotFoundException {
        List<String[]> stations = new ArrayList<>();

        Scanner scan = new Scanner(new File(arg));

        Pattern regex = Pattern.compile("\\s[-+][0-9]+\\.[0-9]+");

        // Skip first 22 useless lines
        for (int i = 0; i < 22; i++) {
            scan.nextLine();
        }

        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            if (line.isEmpty()) continue;

            int i = line.indexOf(" ");
            i = line.indexOf(" ", i + 1);
            String usaf = line.substring(0, i).replaceAll("\\s+", "");

            Matcher matcher = regex.matcher(line);

            if (!matcher.find()) continue;
            String lat = line.substring(matcher.start() + 1, matcher.end());

            if (!matcher.find()) continue;
            String lon = line.substring(matcher.start() + 1, matcher.end());

            stations.add(new String[]{usaf, lat, lon});
        }

        scan.close();

        return stations;
    }

}
