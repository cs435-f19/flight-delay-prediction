package datajoin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataJoinMain {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Missing args:   AIRPORT_STATIONS_FILE WEATHER_FILE DELAYS_FILE");

            System.exit(1);
        }

        long t = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();

        Dataset<Row> joined = loadAndJoinAllDatasets(args[0], args[1], args[2], spark);

        joined.show(100);

        if (args.length >= 4) joined.coalesce(1).write().mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs", false).option("header", true).csv(args[3]);

        System.out.println("Runtime: " + (System.currentTimeMillis() - t) / 1000.0 + " seconds");
    }

    private static Dataset<Row> loadAndJoinAllDatasets(String airportStationsFile, String weatherFolder, String delaysFolder, SparkSession spark) throws IOException {
        // Load airport-stations dataset
        Dataset<Row> airportStationsCSV = loadAirportStationsDataset(airportStationsFile, spark);

        // Load delay dataset from CSV file
        Dataset<Row> delayCSV = loadDelaysDatasets(delaysFolder, spark);

        // Join airport-stations with delays
        Dataset<Row> delayAirportJoined = joinAirportStationsWithDelaysDataset(airportStationsCSV, delayCSV);

        // Load weather datasets from a CSV file
        Dataset<Row> weatherCSV = loadWeatherDatasets(weatherFolder, spark);

        // Join weather dataset with airport-stations dataset on station.
        return joinWeatherWithDelays(delayAirportJoined, weatherCSV);
    }

    private static Dataset<Row> joinWeatherWithDelays(Dataset<Row> delayAirportJoined, Dataset<Row> weatherCSV) {
        weatherCSV.cache();
        Dataset<Row> tempWeather = weatherCSV.withColumnRenamed("TEMP", "ORIGIN_TEMP").withColumnRenamed("DEWP", "ORIGIN_DEWP").withColumnRenamed("SLP", "ORIGIN_SLP").withColumnRenamed("STP", "ORIGIN_STP").withColumnRenamed("VISIB", "ORIGIN_VISIB").withColumnRenamed("WDSP", "ORIGIN_WDSP").withColumnRenamed("MXSPD", "ORIGIN_MXSPD").withColumnRenamed("GUST", "ORIGIN_GUST").withColumnRenamed("MAX", "ORIGIN_MAX").withColumnRenamed("MIN", "ORIGIN_MIN").withColumnRenamed("PRCP", "ORIGIN_PRCP").withColumnRenamed("SNDP", "ORIGIN_SNDP");
        Dataset<Row> joined = delayAirportJoined.join(tempWeather, delayAirportJoined.col("ORIGIN_STATION").equalTo(tempWeather.col("STATION")).and(delayAirportJoined.col("FL_DATE").equalTo(tempWeather.col("DATE"))));
        joined = joined.drop("STATION", "DATE");

        tempWeather = weatherCSV.withColumnRenamed("TEMP", "DEST_TEMP").withColumnRenamed("DEWP", "DEST_DEWP").withColumnRenamed("SLP", "DEST_SLP").withColumnRenamed("STP", "DEST_STP").withColumnRenamed("VISIB", "DEST_VISIB").withColumnRenamed("WDSP", "DEST_WDSP").withColumnRenamed("MXSPD", "DEST_MXSPD").withColumnRenamed("GUST", "DEST_GUST").withColumnRenamed("MAX", "DEST_MAX").withColumnRenamed("MIN", "DEST_MIN").withColumnRenamed("PRCP", "DEST_PRCP").withColumnRenamed("SNDP", "DEST_SNDP");
        joined = joined.join(tempWeather, joined.col("DEST_STATION").equalTo(tempWeather.col("STATION")).and(joined.col("FL_DATE").equalTo(tempWeather.col("DATE"))));
        joined = joined.drop("FL_DATE", "STATION");

        // Fill empty fields with 0.0
        joined = joined.na().fill(0.0, new String[]{"CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"});

        // TODO fill bad values of weather? (999.9 = no existing value, according to NOAA)
        Map<Double, Double> replaceMap = new HashMap<>();
        replaceMap.put(999.9, 0.0);
        joined = joined.na().replace(new String[]{"ORIGIN_DEWP", "ORIGIN_SLP", "ORIGIN_STP", "ORIGIN_VISIB", "ORIGIN_WDSP", "ORIGIN_MXSPD", "ORIGIN_GUST", "ORIGIN_MAX", "ORIGIN_MIN", "ORIGIN_PRCP", "ORIGIN_SNDP", "DEST_DEWP", "DEST_SLP", "DEST_STP", "DEST_VISIB", "DEST_WDSP", "DEST_MXSPD", "DEST_GUST", "DEST_MAX", "DEST_MIN", "DEST_PRCP", "DEST_SNDP"}, replaceMap);

        joined = joined.persist();

        return joined;
    }

    private static Dataset<Row> loadAirportStationsDataset(String arg, SparkSession spark) {
        StructType airportStationSchema = new StructType().add("airport", "string").add("a_lat", "double").add("a_lon", "double").add("station", "string").add("s_lat", "double").add("s_lon", "double").add("station_dist", "double");
        return spark.read().option("mode", "DROPMALFORMED").schema(airportStationSchema).csv(arg);
    }

    private static Dataset<Row> loadDelaysDatasets(String arg, SparkSession spark) throws IOException {
        List<String> paths = new ArrayList<>();
        Files.find(Paths.get(arg), Integer.MAX_VALUE, (path, attrs) -> attrs.isRegularFile()).forEach(path -> paths.add(path.toString()));

        // Not using schema here because it makes a huge mess
        Dataset<Row> delayCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(0));
        for (int i = 1; i < paths.size(); i++) {
            delayCSV = delayCSV.union(spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(i)));
        }
        delayCSV = delayCSV.filter("CANCELLED==0.0"); // Filter out cancelled flights
        delayCSV = delayCSV.drop("OP_CARRIER", "OP_CARRIER_FL_NUM", "CRS_DEP_TIME", "DEP_TIME", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "Unnamed: 27");
        return delayCSV;
    }

    private static Dataset<Row> joinAirportStationsWithDelaysDataset(Dataset<Row> airportStationsCSV, Dataset<Row> delayCSV) {
        Dataset<Row> tempAirports = airportStationsCSV.withColumnRenamed("a_lat", "ORIGIN_AIRPORT_LAT").withColumnRenamed("a_lon", "ORIGIN_AIRPORT_LONG").withColumnRenamed("s_lat", "ORIGIN_STATION_LAT").withColumnRenamed("s_lon", "ORIGIN_STATION_LONG").withColumnRenamed("station_dist", "ORIGIN_STATION_DIST").withColumnRenamed("station", "ORIGIN_STATION");
        Dataset<Row> delayAirportJoined = tempAirports.join(delayCSV, tempAirports.col("airport").equalTo(delayCSV.col("ORIGIN")));
        delayAirportJoined = delayAirportJoined.drop("airport");

        tempAirports = airportStationsCSV.withColumnRenamed("a_lat", "DEST_AIRPORT_LAT").withColumnRenamed("a_lon", "DEST_AIRPORT_LONG").withColumnRenamed("s_lat", "DEST_STATION_LAT").withColumnRenamed("s_lon", "DEST_STATION_LONG").withColumnRenamed("station_dist", "DEST_STATION_DIST").withColumnRenamed("station", "DEST_STATION");
        delayAirportJoined = tempAirports.join(delayAirportJoined, tempAirports.col("airport").equalTo(delayAirportJoined.col("DEST")));
        delayAirportJoined = delayAirportJoined.drop("airport");

        // TODO: Comment out next line to include dest and origin lat/long
        delayAirportJoined = delayAirportJoined.drop("DEST_AIRPORT_LAT", "DEST_AIRPORT_LONG", "DEST_STATION_LAT", "DEST_STATION_LONG", "ORIGIN_AIRPORT_LAT", "ORIGIN_AIRPORT_LONG", "ORIGIN_STATION_LAT", "ORIGIN_STATION_LONG");

        return delayAirportJoined;
    }

    private static Dataset<Row> loadWeatherDatasets(String arg, SparkSession spark) throws IOException {
        List<String> paths = new ArrayList<>();
        Files.find(Paths.get(arg), Integer.MAX_VALUE, (path, attrs) -> attrs.isRegularFile()).forEach(path -> paths.add(path.toString()));

        Dataset<Row> weatherCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(0));
        for (int i = 1; i < paths.size(); i++) {
            weatherCSV = weatherCSV.union(spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(i)));
        }

        weatherCSV = weatherCSV.drop("LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "TEMP_ATTRIBUTES", "DEWP_ATTRIBUTES", "SLP_ATTRIBUTES", "STP_ATTRIBUTES", "VISIB_ATTRIBUTES", "WDSP_ATTRIBUTES", "MAX_ATTRIBUTES", "MIN_ATTRIBUTES", "PRCP_ATTRIBUTES", "FRSHTT");

        return weatherCSV;
    }

}
