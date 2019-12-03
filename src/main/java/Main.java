import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("PageRankWikiBomb").config("spark.master", "local").getOrCreate();

        // TODO
    }

}
