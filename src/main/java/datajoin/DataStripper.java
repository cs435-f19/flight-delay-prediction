package datajoin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataStripper {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();

        Dataset<Row> data = spark.read().option("mode", "DROPMALFORMED").option("inferSchema", true).option("header", true).csv(args[0]);

        data = data.drop("DEST_STATION", "DEST_STATION_DIST", "ORIGIN_STATION", "ORIGIN_STATION_DIST", "ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY", "CARRIER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "DATE").persist();

//        Dataset<Row> dest = data.drop("ORIGIN_TEMP", "ORIGIN_DEWP", "ORIGIN_SLP", "ORIGIN_STP", "ORIGIN_VISIB", "ORIGIN_WDSP", "ORIGIN_MXSPD", "ORIGIN_GUST", "ORIGIN_MAX", "ORIGIN_MIN", "ORIGIN_PRCP", "ORIGIN_SNDP");
        Dataset<Row> origin = data.drop("DEST_TEMP", "DEST_DEWP", "DEST_SLP", "DEST_STP", "DEST_VISIB", "DEST_WDSP", "DEST_MXSPD", "DEST_GUST", "DEST_MAX", "DEST_MIN", "DEST_PRCP", "DEST_SNDP");

//        dest.coalesce(8).write().mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs", false).option("header", true).csv(args[1] + "-dest");
        origin.coalesce(8).write().mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs", false).option("header", true).csv(args[1]);

//        Runtime.getRuntime().exec("rm " + args[1] + "-dest/.part*");
////        Runtime.getRuntime().exec("rm " + args[1] + "-origin/.part*");
//
//        OutputCombiner.main(new String[]{args[1] + "-dest", args[2] + "-dest"});
////        OutputCombiner.main(new String[]{args[1] + "-origin", args[2] + "-origin"});
//
//        Runtime.getRuntime().exec("rm -rf " + args[1] + "-dest");
////        Runtime.getRuntime().exec("rm -rf " + args[1] + "-origin");
//
//        Runtime.getRuntime().exec("python3 csvtolibsvm.py " + args[2] + "-dest " + args[3] + "-dest 0 True");
////        Runtime.getRuntime().exec("python3 csvtolibsvm.py " + args[2] + "-origin " + args[3] + "-origin 0 True");

    }

}
