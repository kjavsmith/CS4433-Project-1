import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TaskD {
    public static class computeFriendsMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable p2User = new IntWritable();
        private final static IntWritable one = new IntWritable(1);
        private boolean firstRowProcessed = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!firstRowProcessed) {
                firstRowProcessed = true;
                return;
            }
            String[] friendInfo = value.toString().split(",");
            try {
                int currentUserId = Integer.parseInt(friendInfo[2].trim());
                p2User.set(currentUserId);
                context.write(p2User, one);
            } catch (NumberFormatException e) {
                System.err.println("Invalid");
            }
        }
    }

    public static class computeFriendReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int countValue = 0;
            for (IntWritable val : values) {
                countValue ++;
            }
            context.write(key, new IntWritable(countValue));
        }
    }

    public static class computeJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Map<Integer, String> userIdToName = new HashMap<>();
        private Map<Integer, Integer> userIdToCount = new HashMap<>();
        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No file found");
            }

            for (URI fileUri : cacheFiles) {
                File file = new File(fileUri.getPath());
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String currLine;
                    boolean firstRowComputed = false;
                    while ((currLine = reader.readLine()) != null) {
                        if (!firstRowComputed) {
                            firstRowComputed = true;
                            continue;
                        }
                        String[] currentUserInfo = currLine.split(",");
                        int currentUserId = Integer.parseInt(currentUserInfo[0]);
                        String userName = currentUserInfo[1];

                        userIdToName.put(currentUserId, userName);
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] currentFriendRow = value.toString().split("\t");
            int currentUserId = Integer.parseInt(currentFriendRow[0]);
            int currentUserCount = Integer.parseInt(currentFriendRow[1]);
            userIdToCount.put(currentUserId, currentUserCount);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> userEntry : userIdToName.entrySet()) {
                int currentUSerId = userEntry.getKey();
                String currentUserName = userEntry.getValue();
                int userCount = userIdToCount.getOrDefault(currentUSerId, 0);
                context.write(new Text(currentUserName), new IntWritable(userCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job firstJob = Job.getInstance(conf, "Compute Friend Count");
        firstJob.setJarByClass(TaskD.class);
        firstJob.setMapperClass(computeFriendsMap.class);
        firstJob.setReducerClass(computeFriendReducer.class);
        firstJob.setOutputKeyClass(IntWritable.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/friends.csv"));
        FileOutputFormat.setOutputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/friends_count"));

        if (!firstJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job secondJob = Job.getInstance(conf, "Compute Page Join");
        secondJob.setJarByClass(TaskD.class);
        secondJob.addCacheFile(new URI("file:///C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
        secondJob.setMapperClass(computeJoinMapper.class);
        secondJob.setNumReduceTasks(0);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/friends_count"));
        FileOutputFormat.setOutputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_d_final_output"));

        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }
}
