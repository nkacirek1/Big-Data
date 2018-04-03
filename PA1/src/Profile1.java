import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Profile 1: A list of unigrams that occurred at least once in the entire corpus. Eliminate duplicates.
 * output should be each of the unigrams by themselves on an individual line
 **/

public class Profile1 {

    // Profile 1 Mapper:
    // reads in the input, parses it to figure out if it is a title or not,
        // gets rid of all other characters and converts it all to lowercase
    // outputs each word as key, and empty string as value
    public static class P1Mapper extends Mapper<Object, Text, Text, Text>{

        private Text unigram = new Text();
        private Text blank = new Text("");

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
                context.write(unigram, blank);

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

    //27 partitioners
    //grabs the first letter from the unigram
        //one partition for each letter of the alphabet, and one for digits
    public static class P1Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            Character sliceKey = key.toString().charAt(0);
            if(sliceKey >= '0' && sliceKey <= '9'){
                return 0;
            }

            int hash = Math.abs(sliceKey.hashCode());
            return (hash % 'a')+1;
        }
    }

    //outputs the unique unigram, and a blank Text
    public static class P1Reducer extends Reducer<Text,Text,Text,Text> {

        private Text blank = new Text("");

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Context generates an output key/value pair
            context.write(key, blank);
        }
    }

    public static void main(String[] args) throws Exception { Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Profile 1");
        job.setJarByClass(Profile1.class);
        job.setMapperClass(P1Mapper.class);
        job.setCombinerClass(P1Reducer.class);
        job.setNumReduceTasks(27);
        job.setPartitionerClass(P1Partitioner.class);
        job.setReducerClass(P1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
