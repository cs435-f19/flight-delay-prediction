import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Missing args:   airport-weather-stations-file");

            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();

        // Load airport-stations dataset
        StructType airportStationSchema = new StructType().add("airport", "string").add("a_lat", "double").add("a_lon", "double").add("station", "string").add("s_lat", "double").add("s_lon", "double").add("station_dist", "double");
        Dataset<Row> airportStationsCSV = spark.read().option("mode", "DROPMALFORMED").schema(airportStationSchema).csv(args[0]);

//        System.out.println("Airports: " + airportStationsCSV.count());
//        for (Row row : airportStationsCSV.collectAsList()) {
//            System.out.println(row);
//        }

        // Load a single weather dataset from a CSV file
        Dataset<Row> weatherCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(args[1]);
        weatherCSV = weatherCSV.drop("LATITUDE").drop("LONGITUDE").drop("ELEVATION").drop("NAME").drop("TEMP_ATTRIBUTES").drop("DEWP_ATTRIBUTES").drop("SLP_ATTRIBUTES").drop("STP_ATTRIBUTES").drop("VISIB_ATTRIBUTES").drop("WDSP_ATTRIBUTES").drop("MAX_ATTRIBUTES").drop("MIN_ATTRIBUTES").drop("PRCP_ATTRIBUTES").drop("FRSHTT");

//        System.out.println("Weather records: " + weatherCSV.count());
//        for (Row row : weatherCSV.collectAsList()) {
//            System.out.println(row);
//        }

        // Join weather dataset with airport-stations dataset on station.
        Dataset<Row> joined = airportStationsCSV.join(weatherCSV, airportStationsCSV.col("station").equalTo(weatherCSV.col("STATION")));

//        System.out.println("Joined: " + joined.count());
//        System.out.println(Arrays.toString(joined.columns()));
//        for (Row row : joined.collectAsList()) {
//            System.out.println(row);
//        }
    }

}
