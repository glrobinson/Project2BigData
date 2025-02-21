package Problem1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Step2a {
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Hardcoded centroids
            centroids.add(new double[]{1000, 1000});
            centroids.add(new double[]{3000, 3000});
            centroids.add(new double[]{5000, 5000});
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
            if (tokenizer.countTokens() != 2) return;

            double x = Double.parseDouble(tokenizer.nextToken());
            double y = Double.parseDouble(tokenizer.nextToken());

            // Find the nearest centroid
            double minDistance = Double.MAX_VALUE;
            String nearestCentroid = "";

            for (double[] centroid : centroids) {
                double distance = Math.sqrt(Math.pow((centroid[0] - x), 2) + Math.pow((centroid[1] - y), 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroid = centroid[0] + "," + centroid[1];
                }
            }

            // Emit (nearest centroid, data point)
            context.write(new Text(nearestCentroid), new Text(x + "," + y));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0, sumY = 0;
            int count = 0;

            for (Text value : values) {
                StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
                if (tokenizer.countTokens() != 2) continue;

                sumX += Double.parseDouble(tokenizer.nextToken());
                sumY += Double.parseDouble(tokenizer.nextToken());
                count++;
            }

            if (count > 0) {
                double newX = sumX / count;
                double newY = sumY / count;
                context.write(key, new Text(newX + "," + newY));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("max.iterations", 1); // Run only 1 iteration

        Job job = Job.getInstance(conf, "Single Iteration KMeans");
        job.setJarByClass(Step2a.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project2_BigData/Project2/data_points.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project2_BigData/Project2/outputProblem1/step2a"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
