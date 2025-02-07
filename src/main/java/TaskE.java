
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskE {
    public static class computeAccessMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable userId = new IntWritable();
        private IntWritable pageId = new IntWritable();
        private boolean firstRowProcessed = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!firstRowProcessed) {
                firstRowProcessed = true;
                return;
            }
            String[] currentAccessRow = value.toString().split(",");
            try {
                int currentUserId = Integer.parseInt(currentAccessRow[1].trim());
                int currentPageId = Integer.parseInt(currentAccessRow[2].trim());
                userId.set(currentUserId);
                pageId.set(currentPageId);
                context.write(userId, pageId);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class computeAccessLogreducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int accessCount = 0;
            HashSet<Integer> pageSet = new HashSet<>();

            for (IntWritable val : values) {
                accessCount++;
                pageSet.add(val.get());
            }
            context.write(key, new Text(accessCount + "\t" + pageSet.size()));
        }
    }

    public static class computeJoinReducerMapper extends Mapper<Object, Text, Text, Text> {
        private Map<Integer, String> idToName = new HashMap<>();
        private boolean headerProcessed = false;
        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No file available");
            }
            for (URI fileUri : cacheFiles) {
                File file = new File(fileUri.getPath());
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String currentLine;
                    boolean firstRowProcessed = false;
                    while ((currentLine = reader.readLine()) != null) {
                        if (!firstRowProcessed) {
                            firstRowProcessed = true;
                            continue;
                        }
                        String[] currentUserInfo = currentLine.split(",");
                        if (currentUserInfo.length < 2) continue;
                        int userID = Integer.parseInt(currentUserInfo[0]);
                        String userName = currentUserInfo[1];
                        idToName.put(userID, userName);
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!headerProcessed) {
                context.write(new Text("Name"), new Text("Total Access\tDistinct Pages"));
                headerProcessed = true;
            }
            String[] currentUserInfo = value.toString().split("\t");
            if (currentUserInfo.length < 3) return;

            int userID = Integer.parseInt(currentUserInfo[0]);
            String userName = idToName.getOrDefault(userID, "Unknown");

            context.write(new Text(userName), new Text(currentUserInfo[1] + "\t" + currentUserInfo[2]));

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf, "Compute Page Access Counts");
        firstJob.setJarByClass(TaskE.class);
        firstJob.setMapperClass(computeAccessMapper.class);
        firstJob.setReducerClass(computeAccessLogreducer.class);
        firstJob.setOutputKeyClass(IntWritable.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv"));
        FileOutputFormat.setOutputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/access_counts"));

        if (!firstJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job secondJob = Job.getInstance(conf, "Compute Join User Names");
        secondJob.setJarByClass(TaskE.class);
        secondJob.addCacheFile(new URI("file:///C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
        secondJob.setMapperClass(computeJoinReducerMapper.class);
        secondJob.setNumReduceTasks(0);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/access_counts"));
        FileOutputFormat.setOutputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/final_task_e_computation"));

        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}
