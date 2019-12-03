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
        // TODO Init null values to 0
//        delayCSV.show(100);


        // TODO Join with destination as well
        // TODO Rename columns of join to avoid duplicate columns
        Dataset<Row> delayAirportJoined = airportStationsCSV.join(delayCSV, airportStationsCSV.col("airport").equalTo(delayCSV.col("ORIGIN")));
//        delayAirportJoined.show(100);


        // Load a single weather dataset from a CSV file
        Dataset<Row> weatherCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(args[1]);
        weatherCSV = weatherCSV.drop("LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "TEMP_ATTRIBUTES", "DEWP_ATTRIBUTES", "SLP_ATTRIBUTES", "STP_ATTRIBUTES", "VISIB_ATTRIBUTES", "WDSP_ATTRIBUTES", "MAX_ATTRIBUTES", "MIN_ATTRIBUTES", "PRCP_ATTRIBUTES", "FRSHTT");
//        weatherCSV.show(100);


        // Join weather dataset with airport-stations dataset on station.
        Dataset<Row> joined = delayAirportJoined.join(weatherCSV, delayAirportJoined.col("station").equalTo(weatherCSV.col("STATION")).and(delayAirportJoined.col("FL_DATE").equalTo(weatherCSV.col("DATE")))).drop("STATION");
        joined = joined.drop("FL_DATE", "airport", "a_lat", "a_lon", "s_lat", "s_lon"); // TODO delete this line
        joined.show(100);
    }

}
