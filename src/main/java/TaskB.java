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
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class TaskB {

    public static class AccessMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private IntWritable currentPageId = new IntWritable();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] accessInfo = value.toString().split(",");

            try {
                int currPageVal = Integer.parseInt(accessInfo[2]);
                currentPageId.set(currPageVal);
                context.write(currentPageId, one);
            } catch (NumberFormatException e) {

            }


        }
    }

    public static class computePageReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int currentSum = 0;
            for (IntWritable val : values) {
                currentSum += val.get();
            }
            context.write(key, new IntWritable(currentSum));
        }
    }

    public static class computeTop10 extends Mapper<Object, Text, IntWritable, IntWritable> {
        private Map<Integer, Integer> pageToCount = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pageInfo = value.toString().split("\t");
            if (pageInfo.length < 2) return ;

            int currentId = Integer.parseInt(pageInfo[0]);
            int currentCount = Integer.parseInt(pageInfo[1]);
            pageToCount.put(currentId, currentCount);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<Integer, Integer>> topTenPages = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());
            topTenPages.addAll(pageToCount.entrySet());
            int currentCount = 0;
            while (!topTenPages.isEmpty() && currentCount < 10) {
                Map.Entry<Integer, Integer> currentPage = topTenPages.poll();
                context.write(new IntWritable(currentPage.getKey()), new IntWritable(currentPage.getValue()));
                currentCount++;
            }
        }
    }

    public static class computeJoinReducer extends Reducer<IntWritable, IntWritable, Text, Text>{
        private Map<Integer, String[]> idToInfo = new HashMap<>();

        public void setup(Context context) throws IOException {
            System.out.println("Running setup()...");  // This should always print
            URI[] cacheFile = context.getCacheFiles();
            if (cacheFile == null || cacheFile.length == 0) {
                System.out.println("No cache file found;");
                return;
            }
            System.out.println("Cache files found: " + cacheFile.length);  // Debugging output


            for (URI currFile : cacheFile) {
                try(BufferedReader reader = new BufferedReader(new FileReader(new File(currFile)))) {
                    String currLine;
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
            System.out.println("Loaded " + idToInfo.size() + " records from pages.csv"); // Debugging

        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] currentInfo = idToInfo.get(key.get());
            context.write(new Text(key.toString()), new Text(currentInfo[0] + "\t" + currentInfo[1]));
        }
    }



//    public void debug(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(TaskB.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "first job");
        job1.setJarByClass(TaskB.class);
        job1.setMapperClass(AccessMapper.class);
        job1.setReducerClass(computePageReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/access_logs.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_count"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Compute top 10");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(computeTop10.class);
        job2.setReducerClass(Reducer.class); // Identity reducer
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_count"));
        FileOutputFormat.setOutputPath(job2, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_top_10"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }


        Job job3 = Job.getInstance(conf, "Join Mypage Table");
        job3.setJarByClass(TaskB.class);
        job3.addCacheFile(new URI("file:///C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/pages.csv"));
        job3.setMapperClass(Mapper.class);
        job3.setReducerClass(computeJoinReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_compute_top_10"));
        FileOutputFormat.setOutputPath(job3, new Path("C:/Users/wpiguest/Desktop/projet1/CS4433-Project-1/output/task_b_final_computation"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
