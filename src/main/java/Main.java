import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Main {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Missing args:   AIRPORT_STATIONS_FILE WEATHER_FILE DELAYS_FILE");

            System.exit(1);
        }

        // TODO Accept multiple delays files
        // TODO Accept multiple weather files
        // TODO Only load weather files for appropriate stations?

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();


        // Load airport-stations dataset
        StructType airportStationSchema = new StructType().add("airport", "string").add("a_lat", "double").add("a_lon", "double").add("station", "string").add("s_lat", "double").add("s_lon", "double").add("station_dist", "double");
        Dataset<Row> airportStationsCSV = spark.read().option("mode", "DROPMALFORMED").schema(airportStationSchema).csv(args[0]);
//        airportStationsCSV.show(100);


        // Load delay dataset from CSV file
        // Not using schema here because it makes a huge mess
        Dataset<Row> delayCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(args[2]);
        delayCSV = delayCSV.filter("CANCELLED==0.0"); // Filter out cancelled flights
        delayCSV = delayCSV.drop("OP_CARRIER", "OP_CARRIER_FL_NUM", "CRS_DEP_TIME", "DEP_TIME", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "Unnamed: 27");
//        delayCSV.show(100);


        Dataset<Row> tempAirports = airportStationsCSV.withColumnRenamed("a_lat", "ORIGIN_AIRPORT_LAT").withColumnRenamed("a_lon", "ORIGIN_AIRPORT_LONG").withColumnRenamed("s_lat", "ORIGIN_STATION_LAT").withColumnRenamed("s_lon", "ORIGIN_STATION_LONG").withColumnRenamed("station_dist", "ORIGIN_STATION_DIST").withColumnRenamed("station", "ORIGIN_STATION");
        Dataset<Row> delayAirportJoined = tempAirports.join(delayCSV, tempAirports.col("airport").equalTo(delayCSV.col("ORIGIN")));
        delayAirportJoined = delayAirportJoined.drop("airport");
        tempAirports = airportStationsCSV.withColumnRenamed("a_lat", "DEST_AIRPORT_LAT").withColumnRenamed("a_lon", "DEST_AIRPORT_LONG").withColumnRenamed("s_lat", "DEST_STATION_LAT").withColumnRenamed("s_lon", "DEST_STATION_LONG").withColumnRenamed("station_dist", "DEST_STATION_DIST").withColumnRenamed("station", "DEST_STATION");
        delayAirportJoined = tempAirports.join(delayAirportJoined, tempAirports.col("airport").equalTo(delayAirportJoined.col("DEST")));
        delayAirportJoined = delayAirportJoined.drop("airport");
        // TODO: Comment out next line to include dest and origin lat/long
        delayAirportJoined = delayAirportJoined.drop("DEST_AIRPORT_LAT", "DEST_AIRPORT_LONG", "DEST_STATION_LAT", "DEST_STATION_LONG", "ORIGIN_AIRPORT_LAT", "ORIGIN_AIRPORT_LONG", "ORIGIN_STATION_LAT", "ORIGIN_STATION_LONG");

//        delayAirportJoined.show(100);


        // Load a single weather dataset from a CSV file
        Dataset<Row> weatherCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(args[1]);
        weatherCSV = weatherCSV.drop("LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "TEMP_ATTRIBUTES", "DEWP_ATTRIBUTES", "SLP_ATTRIBUTES", "STP_ATTRIBUTES", "VISIB_ATTRIBUTES", "WDSP_ATTRIBUTES", "MAX_ATTRIBUTES", "MIN_ATTRIBUTES", "PRCP_ATTRIBUTES", "FRSHTT");
//        weatherCSV.show(100);


        // Join weather dataset with airport-stations dataset on station.
        Dataset<Row> tempWeather = weatherCSV.withColumnRenamed("TEMP", "ORIGIN_TEMP").withColumnRenamed("DEWP", "ORIGIN_DEWP").withColumnRenamed("SLP", "ORIGIN_SLP").withColumnRenamed("STP", "ORIGIN_STP").withColumnRenamed("VISIB", "ORIGIN_VISIB").withColumnRenamed("WDSP", "ORIGIN_WDSP").withColumnRenamed("MXSPD", "ORIGIN_MXSPD").withColumnRenamed("GUST", "ORIGIN_GUST").withColumnRenamed("MAX", "ORIGIN_MAX").withColumnRenamed("MIN", "ORIGIN_MIN").withColumnRenamed("PRCP", "ORIGIN_PRCP").withColumnRenamed("SNDP", "ORIGIN_SNDP");
        Dataset<Row> joined = delayAirportJoined.join(tempWeather, delayAirportJoined.col("ORIGIN_STATION").equalTo(tempWeather.col("STATION")).and(delayAirportJoined.col("FL_DATE").equalTo(tempWeather.col("DATE"))));
        joined = joined.drop("STATION", "DATE");
        tempWeather = weatherCSV.withColumnRenamed("TEMP", "DEST_TEMP").withColumnRenamed("DEWP", "DEST_DEWP").withColumnRenamed("SLP", "DEST_SLP").withColumnRenamed("STP", "DEST_STP").withColumnRenamed("VISIB", "DEST_VISIB").withColumnRenamed("WDSP", "DEST_WDSP").withColumnRenamed("MXSPD", "DEST_MXSPD").withColumnRenamed("GUST", "DEST_GUST").withColumnRenamed("MAX", "DEST_MAX").withColumnRenamed("MIN", "DEST_MIN").withColumnRenamed("PRCP", "DEST_PRCP").withColumnRenamed("SNDP", "DEST_SNDP");
        joined = joined.join(tempWeather, joined.col("DEST_STATION").equalTo(tempWeather.col("STATION")).and(joined.col("FL_DATE").equalTo(tempWeather.col("DATE"))));
        joined = joined.drop("FL_DATE", "STATION");
        joined = joined.na().fill(0.0, new String[]{"CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"});
        // TODO fill bad values of weather? (999.9 = no existing value, according to NOAA)
        joined.show(100);
    }

}
