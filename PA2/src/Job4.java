import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4 {

    /**
     * Job4 Mapper:
     * parses the data
     * Calculates IDF(j) and then Calculate TF(ij) * IDF
     * output: < DocumentID, {unigram, TF-IDFvalue} >
     */

    public static class Job4Mapper extends Mapper<Object, Text, Text, Text>{

        private long N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.N  = context.getConfiguration().getLong(Driver.CountersClass.UpdateCount.N.name(), 0);
        }

        @Override
        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            String docID = "";
            String unigram = "";
            String tf_value = "";
            String ni = "";
            StringTokenizer itr = new StringTokenizer(value.toString());

            //double N = Driver.job2.getCounters().findCounter(Driver.UpdateCount.N).getValue();

            while (itr.hasMoreTokens()) {
                //reads in/parses the data
                unigram = itr.nextToken();
                docID = itr.nextToken();
                tf_value = itr.nextToken();
                ni = itr.nextToken();

                //calculates IDF: IDF(j) = log(N/ni)
                double IDF = Math.log10( this.N / Double.parseDouble(ni) );

                //calculates TF * IDF
                double TF_IDF = Double.parseDouble(tf_value) * IDF;

                context.write(new Text(docID), new Text(unigram + "," + TF_IDF));
            }
        }
    }

    /**
     * Job 4 Partitioner:
     * 30 partitioners
     * partitioned by their docID's hash % numberOfPartitions
     * all unigrams with the same docID will go to the same reducer
     */
    public static class Job4Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;
        }
    }

    /**
     * Job 4 Reducer:
     * Identity Reducer
     * output: < DocumentID, {unigram,TF-IDFvalue} >
     */
    public static class Job4Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // first loop and caching
            for (Text val: values) {
                String[] fields = val.toString().split(",");
                context.write(key, new Text(fields[0] + "\t" + fields[1]));
            }
        }
    }

}
