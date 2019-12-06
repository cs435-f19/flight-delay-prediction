package randomforest;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.regression.RandomForestRegressor;

/**
 * Features list:
 *
 * label : type                        example value
 * ======================================================
 * [Label]WEATHER_DELAY : int          0
 * [0]ORIGIN_TEMP : double             35.8
 * [1]ORIGIN_DEWP : double             24.3
 * [2]ORIGIN_SLP : double              995.9
 * [3]ORIGIN_STP : double              991
 * [4]ORIGIN_VISIB : double            8.1
 * [5]ORIGIN_WDSP : double             4.6
 * [6]ORIGIN_MXSPD : double            12
 * [7]ORIGIN_GUST : double             0
 * [8]ORIGIN_MAX : double              48.9
 * [9]ORIGIN_MIN : double              28
 * [10]ORIGIN_PRCP : double            0.1
 * [11]ORIGIN_SNDP : double            0
 * [12]DEST_TEMP : double              -6.1
 * [13]DEST_DEWP : double              -11.8
 * [14]DEST_SLP : double               1004.8
 * [15]DEST_STP : double               1.5
 * [16]DEST_VISIB : double             6
 * [17]DEST_WDSP : double              15
 * [18]DEST_MXSPD : double             24.1
 * [19]DEST_GUST : double              0
 * [20]DEST_MAX : double               1.4
 * [21]DEST_MIN : double               -16.1
 * [22]DEST_PRCP : double              0
 * [23]DEST_SNDP : double              0
 */
public class RandomForestDelay {


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Missing args:   DELAY_DATASET_LIBSVM");

            System.exit(1);
        }

        long t = System.currentTimeMillis();
//
        SparkSession spark = SparkSession.builder().appName("Flight Delay Dataset Join").config("spark.master", "local").getOrCreate();
//
//        Dataset<Row> dataset = loadDelayDataset(args[0], spark);

        //Current random forest regression made by:
        //https://spark.apache.org/docs/latest/ml-classification-regression.html

        System.out.println("Spark session started.");
        System.out.println("Loading in libsvm file...");

        Dataset<Row> dataset = spark.read().format("libsvm").load(args[0]);

        System.out.println("Finished reading in data!");

        //dataset.show(10); // Pretty-print first 100 elements in dataset.

        System.out.println("Indexing categorical features...");

        //Use VectorIndexer in default mode to automatically identify categorical features and properly index them
        VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(dataset);

        System.out.println("Num features: " + featureIndexer.numFeatures());

        System.out.println("Finished indexing categorical features!");
        System.out.println("Splitting data...");

        //Split data 90/10
        Dataset<Row>[] dataSplit = dataset.randomSplit(new double[] {0.9, 0.1});
        Dataset<Row> trainData = dataSplit[0];
        Dataset<Row> testData = dataSplit[1];

        System.out.println("Finished splitting data!");
        System.out.println("Training random forest...");

        //Create a RandomForest model using the weather data (named label) and the previously indexed features column
        RandomForestRegressor rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxBins(25).setSeed(12345);

        //Create a pipeline to allow the chaining of the indexer and random forest
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {featureIndexer, rf});

        //Use the pipeline to fit training data
        PipelineModel model = pipeline.fit(trainData);

        System.out.println("Finished training random forest!");
        System.out.println("Running random forest against test data...");

        //Do a test data comparison to see the accuracy of this model in raw numbers
        Dataset<Row> predictions = model.transform(testData);

        System.out.println("Testing completed!");
        System.out.println("Calculating RMSE...");

        //Show first 20 rows of relevant data
        predictions.select("prediction", "label", "features").show(20);

        //Take the weather data (named label) and the test data comparison to calculate a Root Means Squared Error
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error = " + rmse);

        //RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
        //System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());

//        //Train a RandomForest model.
//        RandomForestRegressor rf = new RandomForestRegressor().setLabelCol("WEATHER_DELAY").setFeaturesCol("ORIGIN_PRCP");
//
//        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {rf});
//        PipelineModel model = pipeline.fit(trainData);
//        Dataset<Row> predictions = model.transform(testData);
//
//        predictions.show(20);

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
