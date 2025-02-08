import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// Delete output+taskg before running

public class TaskG {
    public static class DisconnectedMapper extends Mapper<Object, Text, Text, Text> {
        private static final String TODAY_STRING = "2025-02-06";
        private static final long FOURTEEN_DAYS_MS = 14L * 24L * 60L * 60L * 1000L;
        private Date today;
        private SimpleDateFormat sdf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                today = sdf.parse(TODAY_STRING + " 00:00:00");
            } catch (ParseException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] fields = line.split(",");
            if (fields.length < 5) return;
            String personID = fields[1].trim();
            String personName = "User#" + personID;
            String dateTime = fields[4].trim();
            Date accessDt;
            try {
                accessDt = sdf.parse(dateTime);
            } catch (ParseException e) {
                return;
            }
            long diff = today.getTime() - accessDt.getTime();
            if (diff >= 0 && diff <= FOURTEEN_DAYS_MS) {
                context.write(new Text(personID), new Text("CONNECTED"));
            } else {
                context.write(new Text(personID), new Text("NAME=" + personName));
            }
        }
    }

    public static class DisconnectedReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean connected = false;
            String name = null;
            for (Text val : values) {
                String s = val.toString();
                if (s.equals("CONNECTED")) {
                    connected = true;
                    break;
                }
                if (s.startsWith("NAME=")) {
                    name = s.substring(5);
                }
            }
            if (!connected && name != null) {
                context.write(key, new Text(name));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(DisconnectedMapper.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_g_final_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}