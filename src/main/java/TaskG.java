import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TaskG {

    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        private static final String TODAY_INPUT_DATE = "2009-02-06";
        private static final long FOURTEEN_DAY_INPUT = 14L * 24L * 60L * 60L * 1000L;
        private Date todayDate;
        private SimpleDateFormat sdf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                todayDate = sdf.parse(TODAY_INPUT_DATE + " 00:00:00");
            } catch (ParseException e) {
                throw new IOException("Invalid date format: " + TODAY_INPUT_DATE);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split(",");
            if (fields.length < 5) return;

            String personID = fields[1].trim(); 
            String dateTime = fields[4].trim(); 

            Date accessDate;
            try {
                accessDate = sdf.parse(dateTime);
            } catch (ParseException e) {
                return; 
            }

            long diff = todayDate.getTime() - accessDate.getTime();
            if (diff <= FOURTEEN_DAY_INPUT) {
                context.write(new Text(personID), new Text("USERCONNECTED"));
            }
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 2) return;

            String personID = fields[0].trim();
            String personName = fields[1].trim();
            context.write(new Text(personID), new Text("USERNAME=" + personName));
        }
    }

    public static class DisconnectedReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isUserConnected = false;
            String isUserName = null;

            for (Text val : values) {
                String s = val.toString();
                if (s.equals("USERCONNECTED")) {
                    isUserConnected = true;
                    break;
                }
                if (s.startsWith("USERNAME=")) {
                    isUserName = s.substring(9);
                }
            }

            if (!isUserConnected && isUserName != null) {
                context.write(key, new Text(isUserName));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String accessInput = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv";
        String pagesInput = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv";
        String outputPath = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_g_final_output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Identify Disconnected Users");
        job.setJarByClass(TaskG.class);
        MultipleInputs.addInputPath(job, new Path(accessInput), TextInputFormat.class, AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path(pagesInput), TextInputFormat.class, PagesMapper.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
