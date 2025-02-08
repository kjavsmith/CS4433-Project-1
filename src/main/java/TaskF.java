import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TaskF {

    // Mapper for Friends dataset
    public static class FriendsMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable p1User = new IntWritable();
        private IntWritable p2User = new IntWritable();
        private boolean firstRowProcessed = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!firstRowProcessed) {
                firstRowProcessed = true;
                return;
            }

            String[] currentRow = value.toString().split(",");
            try {
                int currentUser = Integer.parseInt(currentRow[1].trim());
                int currentUserFriend = Integer.parseInt(currentRow[2].trim());

                p1User.set(currentUser);
                p2User.set(currentUserFriend);

                context.write(p1User, new Text("F-" + p2User.toString()));
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid row: " + value.toString());
            }
        }
    }

    public static class AccessLogMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable p1User = new IntWritable();
        private IntWritable p2User = new IntWritable();
        private boolean firstRowProcessed = false;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!firstRowProcessed) {
                firstRowProcessed = true;
                return;
            }

            String[] userInfo = value.toString().split(",");
            if (userInfo.length < 3) return;

            try {
                int user = Integer.parseInt(userInfo[1].trim());
                int pageOwner = Integer.parseInt(userInfo[2].trim());

                p1User.set(user);
                p2User.set(pageOwner);

                context.write(p1User, new Text("A-" + p2User.toString()));
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid row: " + value.toString());
            }
        }
    }

    public static class UninterestedFriendsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private HashSet<String> friendshipsSet = new HashSet<>();
        private HashSet<String> accessesSet = new HashSet<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String p1User = key.toString();
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("F-")) {
                    friendshipsSet.add(p1User + "-" + value.substring(2));
                } else if (value.startsWith("A-")) {
                    accessesSet.add(p1User + "-" + value.substring(2));
                }
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String relation : friendshipsSet) {
                if (!accessesSet.contains(relation)) {
                    String[] parts = relation.split("-");
                    String output = parts[0] + "\t" + parts[1];
                    context.write(new IntWritable(Integer.parseInt(parts[0])), new Text(output));
                }
            }
        }
    }

    public static class JoinNamesMapper extends Mapper<Object, Text, Text, Text> {
        private Map<Integer, String> idToName = new HashMap<>();

        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No file available");
            }

            for (URI fileUri : cacheFiles) {
                File file = new File(fileUri.getPath());
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    boolean firstRowProcessed = false;
                    while ((line = reader.readLine()) != null) {
                        if (!firstRowProcessed) {
                            firstRowProcessed = true;
                            continue;
                        }
                        String[] currentUserInfo = line.split(",");
                        if (currentUserInfo.length < 2) continue;

                        int personID = Integer.parseInt(currentUserInfo[0]);
                        String personName = currentUserInfo[1];
                        idToName.put(personID, personName);
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] currRow = value.toString().split("\t");

            if (currRow.length < 2) return;

            int personID = Integer.parseInt(currRow[1]);
            int neverAccessedID = Integer.parseInt(currRow[2]);
            String personName = idToName.getOrDefault(personID, "Unknown");
            String neverAccessedName = idToName.getOrDefault(neverAccessedID, "Unknown");

            context.write(new Text(personID + ", " + personName), new Text("Never accessed " + neverAccessedID + ", " + neverAccessedName));
        }
    }

    public static void main(String[] args) throws Exception {
        String friendsInput = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/friends.csv";
        String accessInput = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv";
        String pagesInput = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv";
        String outputPath = "C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output";

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Compute Uninterested Users");
        job1.setJarByClass(TaskF.class);


        MultipleInputs.addInputPath(job1, new Path(friendsInput), TextInputFormat.class, FriendsMapper.class);

        MultipleInputs.addInputPath(job1, new Path(accessInput), TextInputFormat.class, AccessLogMapper.class);

        job1.setReducerClass(UninterestedFriendsReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/task_f_friends_output"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Compute Join User Names");
        job3.setJarByClass(TaskF.class);

        job3.addCacheFile(new Path(pagesInput).toUri());

        job3.setMapperClass(JoinNamesMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(outputPath + "/task_f_friends_output"));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/task_f_final_output"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
