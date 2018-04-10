import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;
import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception{
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load input data.
        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(args[1]);
    }

}

