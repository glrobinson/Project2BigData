package Problem2;

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
import java.util.ArrayList;

public class TaskE {
    public static class AccessMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Get user IDs mapped to the pages they accessed
            Integer userID = Integer.parseInt(fields[1]);
            String pageID = fields[2];

            // write the key value pairs
            context.write(new IntWritable(userID), new Text(pageID));
        }
    }

    public static class AccessReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Int to count total pages visited
            int totalPages = 0;
            ArrayList<String> distinctPages = new ArrayList<>();

            for (Text v : values) {
                totalPages++;
                String page = v.toString();
                if (!distinctPages.contains(page)) {
                    distinctPages.add(page);
                }
            }

            // Create result string which includes <total pages visited>,<total unique pages visited>
            String result = totalPages + "," + distinctPages.size();

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorites");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(TaskE.AccessMapper.class);
        job.setReducerClass(TaskE.AccessReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputE"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
