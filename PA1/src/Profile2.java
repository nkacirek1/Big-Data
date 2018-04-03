import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Profile2 {

    // composite key class for first job
    // stores DocID and Unigram
        // compares them using the compareTo method
    public static class CompositeGroupKeyT implements WritableComparable<CompositeGroupKeyT> {

        Text DocumentID;
        Text Unigram;

        public CompositeGroupKeyT(String docID, String oth){
            DocumentID = new Text(docID);
            Unigram = new Text(oth);
        }

        public CompositeGroupKeyT(){
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
        public int compareTo(CompositeGroupKeyT pop) {
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

    // composite key class for second job
    // stores DocID and Frequency
        // compares them using the compareTo method
    public static class CompositeGroupKeyIW implements WritableComparable<CompositeGroupKeyIW> {

        Text DocumentID;
        IntWritable Count;

        public CompositeGroupKeyIW(String docID, String oth){
            DocumentID = new Text(docID);
            Count = new IntWritable(Integer.parseInt(oth));
        }

        public CompositeGroupKeyIW(){
            DocumentID = new Text();
            Count = new IntWritable();
        }

        public void write(DataOutput out) throws IOException {
            DocumentID.write(out);
            Count.write(out);
        }
        public void readFields(DataInput in) throws IOException {
            DocumentID.readFields(in);
            Count.readFields(in);
        }
        public int compareTo(CompositeGroupKeyIW pop) {
            if (pop == null)
                return 0;
            int intcnt = DocumentID.toString().compareTo(pop.DocumentID.toString());
            if(intcnt == 0){
                return  -1 * Count.compareTo(pop.Count);
            }
            else return intcnt;
        }
        @Override
        public String toString() {
            return DocumentID.toString() + "\t" + Count.toString();
        }
    }


    //First Mapper:
    // parses the data the same way as Profile 1, except stores the documentID rateher than skipping over it
    // creates a CompositeGroupKey for the docID and unigram
    // output: <CompositeGroupKey, 1>
    public static class P2InitalMapper extends Mapper<Object, Text, CompositeGroupKeyT, IntWritable>{

        private CompositeGroupKeyT compositeKey;
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            boolean newArticle = true;
            String lastToken = "";
            String docID_ = "";

            //read and line and tokenize the string
            StringTokenizer itr = new StringTokenizer(value.toString());
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
                compositeKey = new CompositeGroupKeyT(docID_, uniToken);

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

    //30 partitioners
    // partitioned by their first characters hash % numberOfPartitions
        // don't have to keep an order in the reducers for the output
    public static class P2InitalPartitioner extends Partitioner<CompositeGroupKeyT, IntWritable>{

        public int getPartition(CompositeGroupKeyT key, IntWritable Value, int numPartitions){

            Character sliceKey = key.Unigram.toString().charAt(0);
            int hash = Math.abs(sliceKey.hashCode());
            return hash % numPartitions;
        }
    }

    //reducer sums all the counts for the unigrams per documentID
    //outputs: <CompositeKey, result>
    public static class P2CountReducer extends Reducer<CompositeGroupKeyT,IntWritable,CompositeGroupKeyT,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(CompositeGroupKeyT key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }



    //BEGINNING OF THE SECOND JOB

    //Second Mapper: reads the intermediate file of DocID\tunigram\tfrequency
    // switches the unigram and frequency in order to sort by frequency
        //creates a CompoisteGroupKey that stores the DocID and the frequency
    // outputs: <CompositeGroupKey, unigram>
    public static class P2ReverseMapper extends Mapper<Object, Text, CompositeGroupKeyIW, Text>{

        private CompositeGroupKeyIW compositeKey;

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            boolean doc = true;
            boolean uni = false;
            String docID = "";
            String unigram = "";
            String count = "";
            //read and line and tokenize the string
            StringTokenizer itr = new StringTokenizer(value.toString());
            //while there are more tokens
            while (itr.hasMoreTokens()) {
                String uniToken = itr.nextToken();
                if(doc){
                    docID = uniToken;
                    uni = true;
                    doc = false;
                }
                else if(uni){
                    unigram = uniToken;
                    uni = false;
                }
                else {
                    count = uniToken;
                    compositeKey = new CompositeGroupKeyIW(docID, count);
                    context.write(compositeKey, new Text(unigram));
                    doc = true;
                    uni = false;
                }
            }
        }
    }


    //30 partitioners
    // partitioned by their docID's hash % numberOfPartitions
        // all unigrams with the same DOCID will go to the same reducer
    public static class P2FinalPartitioner extends Partitioner<CompositeGroupKeyIW, Text>{

        public int getPartition(CompositeGroupKeyIW key, Text value, int numPartitions){

            int hash = Math.abs(key.DocumentID.hashCode());
            return hash % numPartitions;
        }
    }


    //uses compare to method to compare the keys based on descending frequency
    public static class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
            super(CompositeGroupKeyIW.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeGroupKeyIW k1 = (CompositeGroupKeyIW) w1;
            CompositeGroupKeyIW k2 = (CompositeGroupKeyIW) w2;

            return k1.compareTo(k2);
        }
    }

    // job2 reducer:
    // output<docID, unigram \t frequency>
    public static class P2DescSortReducer extends Reducer<CompositeGroupKeyIW, Text, Text, Text> {

        public void reduce(CompositeGroupKeyIW key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(new Text(key.DocumentID.toString()), new Text(val.toString() + "\t" + key.Count.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Profile 2 Job 1");
        job1.setJarByClass(Profile2.class);
        job1.setMapperClass(P2InitalMapper.class);
        job1.setCombinerClass(P2CountReducer.class);
        job1.setNumReduceTasks(30);
        job1.setPartitionerClass(P2InitalPartitioner.class);
        job1.setReducerClass(P2CountReducer.class);
        job1.setOutputKeyClass(CompositeGroupKeyT.class);
        job1.setOutputValueClass(IntWritable.class);

        Job job2 = Job.getInstance(conf, "Profile 2 Job 2");
        job2.setJarByClass(Profile2.class);
        job2.setMapperClass(P2ReverseMapper.class);
        job2.setMapOutputKeyClass(CompositeGroupKeyIW.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(30);
        job2.setPartitionerClass(P2FinalPartitioner.class);
        job2.setSortComparatorClass(CompositeKeyComparator.class);
        job2.setReducerClass(P2DescSortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));
        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        if(job1.waitForCompletion(true)){
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}

