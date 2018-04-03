import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;

public class Job1 {

    /**
     * composite key class for first job
     * stores DocumentID and Unigram
    **/
    public static class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {

        Text DocumentID;
        Text Unigram;

        public CompositeGroupKey(String docID, String oth){
            DocumentID = new Text(docID);
            Unigram = new Text(oth);
        }

        public CompositeGroupKey(){
            DocumentID = new Text();
            Unigram = new Text();
        }

        public void write(DataOutput out) throws IOException {
            DocumentID.write(out);
            Unigram.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            DocumentID.readFields(in);
            Unigram.readFields(in);
        }

        public int compareTo(CompositeGroupKey pop) {
            if (pop == null)
                return 0;
            int intcnt = DocumentID.toString().compareTo(pop.DocumentID.toString());
            if(intcnt == 0){
                return -1 * (Unigram.toString().compareTo(pop.Unigram.toString()));
            }
            else return intcnt;
        }

        @Override
        public String toString() {
            return DocumentID.toString() + "\t" + Unigram.toString();
        }
    }

    /**
     * Job1 Mapper:
     * parses the data
     * creates a CompositeGroupKey for the docID and unigram
     * sets inital count to 1
     * output: < CompositeGroupKey, 1 >
     */
    public static class Job1Mapper extends Mapper<Object, Text, CompositeGroupKey, IntWritable>{

        private CompositeGroupKey compositeKey;
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            //split on periods to get each sentence
            String[] sentences = value.toString().split("\\.");

            boolean newArticle = true;
            String lastToken = "";
            String docID_ = "";

            for(int i = 0; i < sentences.length; i++){
                //read and line and tokenize the string
                StringTokenizer itr = new StringTokenizer(sentences[i]);
                //while there are more tokens
                while (itr.hasMoreTokens()) {
                    String uniToken = itr.nextToken();

                    //if it is a new article then you need to skip over the title
                    //Melissa Paull<====>4788386<====>Melissa
                    if(newArticle){
                        //skip over tokens before first marker is found
                        if(!uniToken.contains("<====>")){
                            continue;
                        }
                        //otherwise you need to grab the text after the second marker as token
                        else{
                            docID_ = uniToken.substring(uniToken.indexOf("<====>")+6,uniToken.lastIndexOf("<====>"));
                            uniToken = uniToken.substring(uniToken.lastIndexOf("<====>"));
                            newArticle = false;
                        }
                    }

                    //convert token to lowercase and get rid of all unwanted punctuation (not the -)
                    uniToken = uniToken.replaceAll("[^A-Za-z0-9]","").toLowerCase();

                    //if empty string then toss
                    if(uniToken.equals("")) {
                        continue;
                    }

                    //makes the composite key and sets it to a text value
                    compositeKey = new CompositeGroupKey(docID_, uniToken);

                    // Context generates an output key/value pair
                    //key is the unigram, the value is 1 for its count
                    context.write(compositeKey, one);

                    //if last token was new line and this token was new line then it will start a new article
                    if(lastToken.equals("\n") && uniToken.equals("\n")){
                        newArticle = true;
                    }
                    else{
                        lastToken = uniToken;
                    }
                }
            }
        }
    }

    /**
     * Job 1 Combiner:
     * same as reducer except keeps them as composite group keys
     */
    public static class Job1Combiner extends Reducer<CompositeGroupKey, IntWritable, CompositeGroupKey, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(CompositeGroupKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Job 1 Partitioner:
     * 30 partitioners
     * partitioned by their docID's hash % numberOfPartitions
     * all unigrams with the same DOCID will go to the same reducer
     */
    public static class Job1Partitioner extends Partitioner<CompositeGroupKey, IntWritable>{

        public int getPartition(CompositeGroupKey key, IntWritable Value, int numPartitions){

            int hash = Math.abs(key.DocumentID.hashCode());
            return hash % numPartitions;
        }
    }

    /**
     * Job 1 Reducer:
     * reducer sums all the counts for the unigrams per documentID
     * outputs: < CompositeKey (DocID, Unigram), {raw term frequency} >
     */
    public static class Job1Reducer extends Reducer<CompositeGroupKey, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(CompositeGroupKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(new Text(key.toString()), result);
        }
    }

}
