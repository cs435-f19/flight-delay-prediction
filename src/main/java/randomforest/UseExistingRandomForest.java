package randomforest;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UseExistingRandomForest {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();

        System.out.println("Spark session started.");
        System.out.println("Loading in libsvm file...");

        Dataset<Row> dataset = spark.read().format("libsvm").load(args[1]);

        System.out.println("Finished reading in data!");
        System.out.println("Loading PipelineModel...");

        PipelineModel model = PipelineModel.load(args[0]);
//        RandomForestModel forestModel = RandomForestModel.load(spark.sparkContext(), args[0]);

        System.out.println("Finished loading model!");
        System.out.println("Predicting delays...");

        Dataset<Row> predictions = model.transform(dataset);
        predictions.select("prediction", "label", "features").show(Math.min(1000, (int) dataset.count()));

        System.out.println("Complete!");
    }

}
