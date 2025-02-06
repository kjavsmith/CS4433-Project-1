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

public class TaskA {

    public static class FacebookUserMapper
            extends Mapper<Object, Text, Text, Text> {


        public static final String NATIONALITY_INPUT = "United States";

        //mAKE LOGIC CHANGE HERE
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] userInput = value.toString().split(",");
            if (userInput.length < 5) return ;


            String userName = userInput[1];
            String userNationality = userInput[2];
            String userHobby = userInput[4];

            if (NATIONALITY_INPUT.equals(userNationality)) {
                context.write(new Text(userName), new Text(userHobby));
            }



        }


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task A");
            job.setJarByClass(TaskA.class);
            job.setMapperClass(FacebookUserMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
            FileOutputFormat.setOutputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output"));
            System.exit(job.waitForCompletion(true) ? 1 : 2);
        }
    }
}