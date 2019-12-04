package randomforest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Columns are case sensitive.
 *
 * Dataset columns:
 *
 * label : type                     example value
 * ==============================================
 * DEST_STATION : long              70063727406
 * DEST_STATION_DIST : double       0.5429855782
 * ORIGIN_STATION : long            70272526491
 * ORIGIN_STATION_DIST : double     1.6574458634
 * ORIGIN : string                  ANC
 * DEST : string                    SCC
 * DEP_DELAY : int                  -6
 * ARR_DELAY : int                  -8
 * CARRIER_DELAY : int              0
 * WEATHER_DELAY : int              0
 * NAS_DELAY : int                  0
 * SECURITY_DELAY : int             0
 * LATE_AIRCRAFT_DELAY : int        0
 * ORIGIN_TEMP : double             35.8
 * ORIGIN_DEWP : double             24.3
 * ORIGIN_SLP : double              995.9
 * ORIGIN_STP : double              991
 * ORIGIN_VISIB : double            8.1
 * ORIGIN_WDSP : double             4.6
 * ORIGIN_MXSPD : double            12
 * ORIGIN_GUST : double             0
 * ORIGIN_MAX : double              48.9
 * ORIGIN_MIN : double              28
 * ORIGIN_PRCP : double             0.1
 * ORIGIN_SNDP : double             0
 * DATE : date                      2011-03-31T00:00:00.000-06:00
 * DEST_TEMP : double               -6.1
 * DEST_DEWP : double               -11.8
 * DEST_SLP : double                1004.8
 * DEST_STP : double                1.5
 * DEST_VISIB : double              6
 * DEST_WDSP : double               15
 * DEST_MXSPD : double              24.1
 * DEST_GUST : double               0
 * DEST_MAX : double                1.4
 * DEST_MIN : double                -16.1
 * DEST_PRCP : double               0
 * DEST_SNDP : double               0
 */
public class RandomForestDelay {


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Missing args:   DELAY_DATASET_CSV");

            System.exit(1);
        }

        long t = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();

        Dataset<Row> dataset = loadDelayDataset(args[0], spark);

        dataset.show(100); // Pretty-print first 100 elements in dataset.

        // TODO random forest implementation

        System.out.println("Runtime: " + (System.currentTimeMillis() - t) / 1000.0 + " seconds");
    }

    /**
     * Loads a dataset of CSV files for the random forest airport delay problem.
     *
     * Examples:
     *      loadDelayDataset("./data/2018.csv", spark); // Loads a single csv file as the dataset
     *
     *      loadDelayDataset("./data", spark); // Loads all csv files in './data' in as a single dataset
     *
     * @param csvPath   Path to .csv file. If path is a folder, recursively gets all .csv files in the folder and appends the datasets to each other.
     * @param spark     Spark context.
     * @return Simple row-column dataset of the .csv dataset
     * @throws IOException When path is invalid or does not exist.
     */
    private static Dataset<Row> loadDelayDataset(String csvPath, SparkSession spark) throws IOException {
        List<String> paths = new ArrayList<>();
        Files.find(Paths.get(csvPath), Integer.MAX_VALUE, (path, attrs) -> attrs.isRegularFile()).forEach(path -> paths.add(path.toString()));

        Dataset<Row> weatherCSV = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(0));
        for (int i = 1; i < paths.size(); i++) {
            weatherCSV = weatherCSV.union(spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(paths.get(i)));
        }

        return weatherCSV.persist();
    }

}
