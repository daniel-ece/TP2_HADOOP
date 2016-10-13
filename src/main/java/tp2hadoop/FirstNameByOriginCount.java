package tp2hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Daniel on 13/10/2016.
 */
public class FirstNameByOriginCount {

    //function mapper that return (key, value), value=1
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1); //value 1
        private Text word = new Text(); //text

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString(); //get the value like (firstname;sexe;nationality1, nationality2, ...;number)

            String origins[] = itr.split(";"); // split by ";"
            String origin[] = origins[2].split(","); // because he can have mutiple nationalities index:2

            //(origin, 1)
            for (String o : origin) {
                word.set(o);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(); //provides access to configuration parameters

        //job allows use to configure the job, submit it, control its execution, and query the state
        Job job = Job.getInstance(conf, "word count"); //create new job
        job.setJarByClass(FirstNameByOriginCount.class); //set the job jar

        job.setMapperClass(FirstNameByOriginCount.TokenizerMapper.class); //mapper
        job.setCombinerClass(FirstNameByOriginCount.IntSumReducer.class); //combiner
        job.setReducerClass(FirstNameByOriginCount.IntSumReducer.class); //reducer

        job.setOutputKeyClass(Text.class); //set the key class for job outputs
        job.setOutputValueClass(IntWritable.class); //set the value class for job outputs

        FileInputFormat.addInputPath(job, new Path(args[0])); //path input
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //path output

        System.exit(job.waitForCompletion(true) ? 0 : 1); //submit the job, then poll for progress until the job is complete
    }
}
