package tp2hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Daniel on 13/10/2016.
 */

//Proportion (in%) of male or female
public class ProportionMaleFemale {

    //function mapper that return (key, value), value=1
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1); //value 1
        private Text word = new Text(); //text

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString(); //get the value like (firstname;sexe;nationality1, nationality2, ...;number)

            String data[] = itr.split(";"); // split by ";"
            String genders[] = data[1].split(","); // gender m or f index:1

            //(gender, 1)
            for (String g : genders) {
                word.set(g);
                context.write(word, one);
            }
        }
    }

    //function reducer that involve all the values corresponding to the same key to a unique pair
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            //simple sum of values
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum); //put the new value
            context.write(key, result); //output (numberOfOrigins, numberOfFirstname)
        }
    }

    public static class PercentageReduce extends Reducer<Text, IntWritable, Text, Text> {
        private FloatWritable percent;
        private long numberInputRecords;

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration(); //get the configuration
            Cluster cluster = new Cluster(conf); //
            Job currentJob = cluster.getJob(context.getJobID()); //get the current job
            numberInputRecords = currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue(); //get the number of input records
        }//source : http://stackoverflow.com/questions/5450290/accessing-a-mappers-counter-from-a-reducer

        //reduce is called for each pair and calculates the percentage for each gender
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            percent = new FloatWritable((float)sum*100/(float)numberInputRecords);
            context.write(key, new Text(percent.toString()+"%"));
        }
    }

    //function main
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(); //provides access to configuration parameters

        //job allows use to configure the job, submit it, control its execution, and query the state
        Job job = Job.getInstance(conf, "ProportionMaleFemale"); //create new job
        job.setJarByClass(ProportionMaleFemale.class); //set the job jar

        job.setMapperClass(ProportionMaleFemale.TokenizerMapper.class); //mapper
        job.setCombinerClass(ProportionMaleFemale.IntSumReducer.class); //combiner
        job.setReducerClass(ProportionMaleFemale.PercentageReduce.class); //reducer

        job.setOutputKeyClass(Text.class); //set the key class for job outputs
        job.setOutputValueClass(IntWritable.class); //set the value class for job outputs

        FileInputFormat.addInputPath(job, new Path(args[0])); //path input
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //path output

        System.exit(job.waitForCompletion(true) ? 0 : 1); //submit the job, then poll for progress until the job is complete
    }
}
