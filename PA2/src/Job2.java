import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Set;

public class Job2 {

    /**
     * Job2 Mapper:
     * parses the data
     * output: < DocumentID, {Unigram,Frequency} >
     */
    public static class Job2Mapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            String docID = "";
            String unigram = "";
            String count = "";
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docID = itr.nextToken();
                unigram = itr.nextToken();
                count = itr.nextToken();
                context.write(new Text(docID), new Text(unigram + ',' + count));
            }
        }
    }

    /**
     * Job 2 Partitioner:
     * 30 partitioners
     * partitioned by their docID's hash % numberOfPartitions
     * all unigrams with the same DOCID will go to the same reducer
     */
    public static class Job2Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;
        }
    }

    /**
     * Job 2 Reducer:
     * 1. Figure out the max raw frequency of any term in the article
     * 2. Compute the TF for each term using the max
     * Output: < DocumentID, {Unigram \t TFvalue}
     */
    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //Increments the count of N by 1 for the docID
            Set<String> uniqueIDs = new HashSet<String>();
            if(!uniqueIDs.contains(key.toString())){
                uniqueIDs.add(key.toString());
                context.getCounter(Driver.CountersClass.UpdateCount.N).increment(1);
            }

            // 1. Figure out the max raw frequency of any term in the article
            double maxFreq = -1.0;

            ArrayList<Text> cache = new ArrayList<Text>();

            // first loop and caching
            for (Text val: values) {
                String[] fields = val.toString().split(",");
                double freq = Integer.parseInt(fields[1]);
                if(freq > maxFreq){
                    maxFreq = freq;
                }
                cache.add(new Text(val.toString()));
            }

            //2. Compute the TF for each term using the max calculated above
            for(Text val : cache){
                //System.out.println("key:" + key.toString() + " value:" + val.toString());
                String[] fields = val.toString().split(",");
                String uni = fields[0];
                double freq = Integer.parseInt(fields[1]);
                double TF = 0.5 + 0.5 * (freq/maxFreq);
                context.write(key, new Text(uni + "\t" + TF));
            }
        }
    }

}
