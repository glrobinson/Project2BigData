package Problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Step2b {
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Read centroids from local file system
            String centroidsFile = context.getConfiguration().get("centroids.path");
            try (BufferedReader reader = new BufferedReader(new FileReader(centroidsFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                    double x = Double.parseDouble(tokenizer.nextToken());
                    double y = Double.parseDouble(tokenizer.nextToken());
                    centroids.add(new double[]{x, y});
                }
            }
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
                context.write(new Text(newX + "," + newY), new Text(""));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        // Use local file system paths instead of HDFS
        String inputPath = "/Users/gracerobinson/Project2_BigData/Project2/data_points.txt";
        String outputPath = "/Users/gracerobinson/Project2_BigData/Project2/outputProblem1/step2b";
        String centroidsPath = "/Users/gracerobinson/Project2_BigData/Project2/centroids.txt";

        Configuration conf = new Configuration();
        int maxIterations = 6; // Set R = 6 iterations
        int iteration = 0;

        while (iteration < maxIterations) {
            System.out.println("Running iteration: " + (iteration + 1));

            // Set current centroids as input
            conf.set("centroids.path", centroidsPath);

            Job job = Job.getInstance(conf, "K-Means Iteration " + (iteration + 1));
            job.setJarByClass(Step2b.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath)); // Input dataset
            Path outputIterationPath = new Path(outputPath + "_iter_" + iteration);
            FileOutputFormat.setOutputPath(job, outputIterationPath);

            job.waitForCompletion(true);

            // Update centroids file for next iteration
            centroidsPath = outputPath + "_iter_" + iteration + "/part-r-00000";

            iteration++;
        }

        System.out.println("K-Means completed after " + maxIterations + " iterations.");
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
