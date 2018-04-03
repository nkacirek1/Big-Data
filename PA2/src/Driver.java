import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Driver {

    public static class CountersClass{
        public enum UpdateCount{
            N
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "PA2 Job 1");
        job1.setJarByClass(Job1.class);
        job1.setMapperClass(Job1.Job1Mapper.class);
        job1.setMapOutputKeyClass(Job1.CompositeGroupKey.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setCombinerClass(Job1.Job1Combiner.class);
        job1.setNumReduceTasks(32);
        job1.setPartitionerClass(Job1.Job1Partitioner.class);
        job1.setReducerClass(Job1.Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        Job job2 = Job.getInstance(conf, "PA2 Job 2");
        job2.setJarByClass(Job2.class);
        job2.setMapperClass(Job2.Job2Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(32);
        job2.setPartitionerClass(Job2.Job2Partitioner.class);
        job2.setReducerClass(Job2.Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Job job3 = Job.getInstance(conf, "PA2 Job 3");
        job3.setJarByClass(Job3.class);
        job3.setMapperClass(Job3.Job3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(32);
        job3.setPartitionerClass(Job3.Job3Partitioner.class);
        job3.setReducerClass(Job3.Job3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        Job job4 = Job.getInstance(conf, "PA2 Job 4");
        job4.setJarByClass(Job4.class);
        job4.setMapperClass(Job4.Job4Mapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(32);
        job4.setPartitionerClass(Job4.Job4Partitioner.class);
        job4.setReducerClass(Job4.Job4Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        Job job5 = Job.getInstance(conf, "PA2 Job 5");
        job5.setJarByClass(Job5.class);
        job5.setNumReduceTasks(32);
        job5.setPartitionerClass(Job5.Job5Partitioner.class);
        job5.setReducerClass(Job5.Job5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp12"));
        FileInputFormat.addInputPath(job2, new Path("temp12"));
        FileOutputFormat.setOutputPath(job2, new Path("temp23"));
        FileInputFormat.addInputPath(job3, new Path("temp23"));
        FileOutputFormat.setOutputPath(job3, new Path("temp34"));
        FileInputFormat.addInputPath(job4, new Path("temp34"));
        FileOutputFormat.setOutputPath(job4, new Path("temp45"));
        MultipleInputs.addInputPath(job5, new Path("temp45"), TextInputFormat.class, Job5.Job5MapperClass1.class);
        MultipleInputs.addInputPath(job5, new Path(args[0]), TextInputFormat.class, Job5.Job5MapperClass2.class);
        FileOutputFormat.setOutputPath(job5, new Path(args[1]));

        if(job1.waitForCompletion(true)){
            if(job2.waitForCompletion(true)){
                Counter count = job2.getCounters().findCounter(CountersClass.UpdateCount.N);
                if(job3.waitForCompletion(true)){
                    job4.getConfiguration().setLong(CountersClass.UpdateCount.N.name(), count.getValue());
                    if(job4.waitForCompletion(true)){


                        System.exit(job5.waitForCompletion(true) ? 0 : 1);
                    }
                }
            }
        }
    }

}
