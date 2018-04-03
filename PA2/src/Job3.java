import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

public class Job3 {

    /**
     * Job3 Mapper:
     * parses the data
     * output: < unigram, {DocumentID,TFvalue} >
     */
    public static class Job3Mapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            String docID = "";
            String unigram = "";
            String tf_value = "";
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docID = itr.nextToken();
                unigram = itr.nextToken();
                tf_value = itr.nextToken();
                context.write(new Text(unigram), new Text(docID + ',' + tf_value));
            }
        }
    }

    /**
     * Job 3 Partitioner:
     * 30 partitioners
     * partitioned by their unigrams's hash % numberOfPartitions
     * all unigrams with the same unigram will go to the same reducer
     */
    public static class Job3Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;
        }
    }

    /**
     * Job 3 Reducer:
     * ni is the number of articles the unigram appears in
     * ni = the size of the iterable list
     * Output: < unigram, {DocumentID, TFvalue, ni}
     */
    public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<Text> cache = new ArrayList<Text>();

            // first loop and caching
            for (Text val: values) {
                cache.add(new Text(val.toString()));
            }

            //grab the size of the list --> this is ni
            int ni = cache.size();

            for (Text val: cache) {
                String[] fields = val.toString().split(",");
                context.write(key, new Text(fields[0] + "\t" + fields[1] + "\t" + ni));
            }

        }
    }

}
