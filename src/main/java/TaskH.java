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
import java.util.HashMap;
import java.util.Map;

// Delete output_taskh before running
public class TaskH {

    public static class PopularMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] fields = line.split(",");
            if (fields.length < 5) return;
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].trim();
            }
            if (fields[0].equalsIgnoreCase("FriendRel")) return;
            context.write(new Text(fields[1]), ONE);
            context.write(new Text(fields[2]), ONE);
        }
    }

    public static class PopularReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Map<String, Integer> counts = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            counts.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int total = 0;
            for (int c : counts.values()) {
                total += c;
            }
            double avg = (double) total / counts.size();
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                if (e.getValue() > avg) {
                    context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);
        job.setMapperClass(PopularMapper.class);
        job.setReducerClass(PopularReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\ashto\\IdeaProjects\\CS4433-Project-1\\friends.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\ashto\\IdeaProjects\\CS4433-Project-1\\output_taskh"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}