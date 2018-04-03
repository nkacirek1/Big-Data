import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile3 {

    // Profile 3 Mapper class:
    // parses the unigrams the same way as Profile 1
    // outputs: <unigram, 1>
    public static class P3InitalMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text unigram = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            boolean newArticle = true;
            String lastToken = "";

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

                //sets the given string as the "unigram" (which will be the key)
                unigram.set(uniToken);

                // Context generates an output key/value pair
                //key is the unigram, the value is 1 for its count
                context.write(unigram, one);

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
    // partitioned by their hash % numberOfPartitions
        // don't have to keep an order in the reducers for the output
    public static class P3InitalPartitioner extends Partitioner<Text, IntWritable>{

        public int getPartition(Text key, IntWritable Value, int numPartitions){

            Character sliceKey = key.toString().charAt(0);

            int hash = Math.abs(sliceKey.hashCode());
            return hash % numPartitions;
        }
    }

    //reducer sums all the counts for the unigrams
    //outputs: <unigram, frequency>
    public static class P3CountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // BEGINNING OF SECOND JOB


    //Second Mapper: reads the intermediate file of unigram\tfrequency
        // switches the unigram and frequency in order to sort by frequency
    // outputs: <frequnecy, unigram>
    public static class P3ReverseMapper extends Mapper<Object, Text, IntWritable, Text>{

        private Text unigram = new Text();
        private static IntWritable count = new IntWritable();

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            boolean uni = true;
            //read and line and tokenize the string
            StringTokenizer itr = new StringTokenizer(value.toString());
            //while there are more tokens
            while (itr.hasMoreTokens()) {
                String uniToken = itr.nextToken();

                if(uni){
                    unigram.set(uniToken);
                    uni = false;
                }
                else {
                    count.set(Integer.parseInt(uniToken));
                    context.write(count, unigram);
                }
            }
        }
    }

    //34 partitions
    //parition in the reverse order (lowest frquency goes to last partitioner
    public static class P3FinalPartitioner extends Partitioner<IntWritable, Text>{

        public int getPartition(IntWritable key, Text Value, int numParitions){

            int sliceKey = key.get();

            if(sliceKey == 1){
                return 33;
            }
            else if(sliceKey == 2){
                return 32;
            }
            else if(sliceKey == 3){
                return 31;
            }
            else if(sliceKey == 4){
                return 30;
            }
            else if(sliceKey == 5){
                return 29;
            }
            else if(sliceKey == 6){
                return 28;
            }
            else if(sliceKey == 7){
                return 27;
            }
            else if(sliceKey == 8){
                return 26;
            }
            else if(sliceKey == 9){
                return 25;
            }
            else if(sliceKey == 10){
                return 24;
            }
            else if(sliceKey == 11){
                return 23;
            }
            else if(sliceKey == 12){
                return 22;
            }
            else if(sliceKey == 13){
                return 21;
            }
            else if(sliceKey == 14){
                return 20;
            }
            else if(sliceKey >= 15 && sliceKey <= 25){
                return 19;
            }
            else if(sliceKey >= 26 && sliceKey <= 40){
                return 18;
            }
            else if(sliceKey >= 41 && sliceKey <= 65){
                return 17;
            }
            else if(sliceKey >= 66 && sliceKey <= 90){
                return 16;
            }
            else if(sliceKey >= 91 && sliceKey <= 125){
                return 15;
            }
            else if(sliceKey >= 126 && sliceKey <= 150){
                return 14;
            }
            else if(sliceKey >= 150 && sliceKey <= 200){
                return 13;
            }
            else if(sliceKey >= 201 && sliceKey <= 300){
                return 12;
            }
            else if(sliceKey >= 301 && sliceKey <= 400){
                return 11;
            }
            else if(sliceKey >= 401 && sliceKey <= 500){
                return 10;
            }
            else if(sliceKey >= 501 && sliceKey <= 600){
                return 9;
            }
            else if(sliceKey >= 601 && sliceKey <= 700){
                return 8;
            }
            else if(sliceKey >= 701 && sliceKey <= 800){
                return 7;
            }
            else if(sliceKey >= 801 && sliceKey <= 900){
                return 6;
            }
            else if(sliceKey >= 901 && sliceKey <= 1000){
                return 5;
            }
            else if(sliceKey >= 1001 && sliceKey <= 1250){
                return 4;
            }
            else if(sliceKey >= 1251 && sliceKey <= 1500){
                return 3;
            }
            else if(sliceKey >= 1501 && sliceKey <= 1750){
                return 2;
            }
            else if(sliceKey >= 2001 && sliceKey <= 2250){
                return 1;
            }
            else{
                return 0;
            }
        }
    }


    //Sort the keys in decreasing order
    //Found on stack overflow for LongWritable, adjusted to IntWritable
    //https://stackoverflow.com/questions/18154686/how-to-implement-sort-in-hadoop

    //compares the keys so that they are sorted in descending order
    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    // outputs: <unigram, frequency>
    public static class P3DescSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Profile 3 Job 1");
        job1.setJarByClass(Profile3.class);
        job1.setMapperClass(P3InitalMapper.class);
        job1.setCombinerClass(P3CountReducer.class);
        job1.setNumReduceTasks(30);
        job1.setPartitionerClass(P3InitalPartitioner.class);
        job1.setReducerClass(P3CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        Job job2 = Job.getInstance(conf, "Profile 3 Job 2");
        job2.setJarByClass(Profile3.class);
        job2.setMapperClass(P3ReverseMapper.class);
        job2.setNumReduceTasks(34);
        job2.setPartitionerClass(P3FinalPartitioner.class);
        job2.setSortComparatorClass(DescendingKeyComparator.class);
        job2.setReducerClass(P3DescSortReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
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
