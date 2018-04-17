import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public final class TaxationPageRank {

    private static Integer lineNumber = 0;

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    //helper method to increment the line number with the titles
    private static Tuple2<String, String> incrementLine(String s){
        lineNumber++;
        return new Tuple2<>(Integer.toString(lineNumber), s);
    }

    public static void main(String[] args) throws Exception {

        //Application initialization
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TaxationPageRank");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read link data set as RDD (Load data)
        JavaRDD<String> lines = sc.textFile(args[0]);

        //Create newRDD,in the form {(A→[B C D], B→[A D], C→[A], D→[B C]} (Preprocessing step)
        //read line and split by colon, the thing on the left is the key and on the right is the group of values
        JavaPairRDD<String, String> links = lines.mapToPair(s -> {
            //String[] parts = SPACES.split(s);
            String[] parts = s.split(":");
            return new Tuple2<>(parts[0], parts[1].trim());
        }).distinct().cache();

        // Initialize ranks of incoming pages to 1.0, to give the form { (A → 1.0), (B → 1.0), (C → 1.0), (D → 1.0) }
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates and updates ranks continuously using PageRank algorithm.
        for (int current = 0; current < 25; current++) {

            //Join; to give form,{ A→([B,C],1.0),B→([A,D],1.0),...}
            // contribs: { (B, _), (C, _), (A, _), (D, _), ... }
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        String[] parsedLinks = s._1.split("\\s");
                        int linkCount = parsedLinks.length;
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : parsedLinks) {
                            results.add(new Tuple2<>(n, s._2() / linkCount));
                        }
                        return results.iterator();
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            //with taxation
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        //read in the titles file
        JavaRDD<String> titleFile = sc.textFile(args[1]);

        //map the title with the line number as the key
        JavaPairRDD<String, String> titles = titleFile.mapToPair(s -> incrementLine(s));

        //inner join the titles and the ranks RDD's to match titles with their page ranks
        //write the values (Title,Page_Rank) to RDD
        JavaRDD<Tuple2<String, Double>> PR_with_title = titles.join(ranks).values();

        //swaps the key and values, then sorts by the PageRank in descending order
        JavaPairRDD<Double, String> swap = PR_with_title.mapToPair(s ->
                new Tuple2<>(s._2, s._1)).sortByKey(false);

        //swap back so will now display Title, PageRank sorted in descending order by PR
        JavaPairRDD<String, Double> finalPageRank = swap.mapToPair(s -> new Tuple2<>(s._2, s._1));

        //output the final RDD to the output file
        finalPageRank.saveAsTextFile(args[2]);

    }
}
