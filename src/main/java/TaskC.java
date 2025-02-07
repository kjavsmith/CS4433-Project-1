import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskC {
    public static class nationalityMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text userNationality = new Text();
        private boolean firstRowProcessed = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!firstRowProcessed) {
                firstRowProcessed = true;
                return;
            }
            String[] userInfo = value.toString().split(",");
            if (userInfo.length < 3) return;
            userNationality.set(userInfo[2].trim());
            context.write(userNationality, one);
        }
    }

    private static class computeNationalityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int countVal = 0;
            for (IntWritable val : values) {
                countVal += val.get();
            }
            context.write(key, new IntWritable(countVal));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Compute Nationality Count");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(TaskC.nationalityMapper.class);
        job.setReducerClass(computeNationalityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_c_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
