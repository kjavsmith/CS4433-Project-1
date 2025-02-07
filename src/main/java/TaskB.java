import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class TaskB {

    public static class PageAccessMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable userPageId = new IntWritable();
        private final static IntWritable one = new IntWritable(1);
        private boolean firstRow = true;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (firstRow) {
                firstRow = false;
                return;
            }

            String[] currentPage = value.toString().split(",");
            if (currentPage.length < 3) return;

            try {
                int currPageVal = Integer.parseInt(currentPage[2].trim());
                userPageId.set(currPageVal);
                context.write(userPageId, one);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid row: " + value.toString());
            }
        }
    }

    public static class computeCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int currSum = 0;
            for (IntWritable currVal : values) {
                currSum += currVal.get();
            }
            context.write(key, new IntWritable(currSum));
        }
    }

    public static class computeTopTen extends Mapper<Object, Text, IntWritable, IntWritable> {
        private Map<Integer, Integer> pageToCount = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] currPageRow = value.toString().split("\t");
            if (currPageRow.length < 2) return;

            int idVal = Integer.parseInt(currPageRow[0]);
            int countVal = Integer.parseInt(currPageRow[1]);
            pageToCount.put(idVal, countVal);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<Integer, Integer>> topTenPage = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());
            topTenPage.addAll(pageToCount.entrySet());

            int currentCountValue = 0;
            while (!topTenPage.isEmpty() && currentCountValue < 10) {
                Map.Entry<Integer, Integer> currentPage = topTenPage.poll();
                context.write(new IntWritable(currentPage.getKey()), new IntWritable(currentPage.getValue()));
                currentCountValue++;
            }
        }
    }


    public static class computeJoinMap extends Mapper<Object, Text, Text, Text> {
        private Map<Integer, String[]> idToInfo = new HashMap<>();

        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No cache files found!");
            }
            for (URI currFile : cacheFiles) {
                File file = new File(currFile.getPath());
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String currLine;
                    currLine = reader.readLine();
                    while ((currLine = reader.readLine()) != null) {
                        String[] currentUserInfo = currLine.split(",");
                        if (currentUserInfo.length < 3) continue;
                        int userPageId = Integer.parseInt(currentUserInfo[0]);
                        String userName = currentUserInfo[1];
                        String userNationality = currentUserInfo[2];
                        idToInfo.put(userPageId, new String[]{userName, userNationality});
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] currentPageInfo = value.toString().split("\t");
            if (currentPageInfo.length < 2) return;

            int pageID = Integer.parseInt(currentPageInfo[0]);
            String[] pageInfo = idToInfo.getOrDefault(pageID, new String[]{"Unknown", "Unknown"});

            context.write(new Text(currentPageInfo[0]), new Text(pageInfo[0] + "\t" + pageInfo[1]));
        }
    }

    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job firstJob = Job.getInstance(conf, "Count Page Computation");
        firstJob.setJarByClass(TaskB.class);
        firstJob.setMapperClass(PageAccessMapper.class);
        firstJob.setReducerClass(computeCountReducer.class);
        firstJob.setOutputKeyClass(IntWritable.class);
        firstJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv"));
        FileOutputFormat.setOutputPath(firstJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_count"));

        if (!firstJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job secondJob = Job.getInstance(conf, "Compute Top 10 Pages");
        secondJob.setJarByClass(TaskB.class);
        secondJob.setMapperClass(computeTopTen.class);
        secondJob.setReducerClass(Reducer.class); // Identity Reducer
        secondJob.setOutputKeyClass(IntWritable.class);
        secondJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_count"));
        FileOutputFormat.setOutputPath(secondJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_top_10"));

        if (!secondJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job thirdJob = Job.getInstance(conf, "Join Mypage Table Computation");
        thirdJob.setJarByClass(TaskB.class);
        thirdJob.addCacheFile(new URI("file:///C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
        thirdJob.setMapperClass(computeJoinMap.class);
        thirdJob.setNumReduceTasks(0); // ✅ Map-Only Job
        thirdJob.setOutputKeyClass(Text.class);
        thirdJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(thirdJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_top_10"));
        FileOutputFormat.setOutputPath(thirdJob, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_final_computation"));

        System.exit(thirdJob.waitForCompletion(true) ? 0 : 1);
    }

}
