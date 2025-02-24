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

public class Step2d {
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
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

            // This is so I can find the nearest centroid
            double minDistance = Double.MAX_VALUE;
            String nearestCentroid = "";

            for (double[] centroid : centroids) {
                double distance = Math.sqrt(Math.pow((centroid[0] - x), 2) + Math.pow((centroid[1] - y), 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroid = centroid[0] + "," + centroid[1];
                }
            }
            context.write(new Text(nearestCentroid), new Text(x + "," + y + ",1"));
        }
    }

    // Combiner yay!
    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0, sumY = 0;
            int count = 0;

            for (Text value : values) {
                StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
                sumX += Double.parseDouble(tokenizer.nextToken());
                sumY += Double.parseDouble(tokenizer.nextToken());
                count += Integer.parseInt(tokenizer.nextToken());
            }

            context.write(key, new Text(sumX + "," + sumY + "," + count));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0;
            double sumY = 0;
            int count = 0;

            for (Text value : values) {
                StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
                sumX += Double.parseDouble(tokenizer.nextToken());
                sumY += Double.parseDouble(tokenizer.nextToken());
                count += Integer.parseInt(tokenizer.nextToken());
            }

            if (count > 0) {
                double newX = sumX / count;
                double newY = sumY / count;
                context.write(new Text(newX + "," + newY), new Text(""));
            }
        }
    }

    // Helper functions
    private static List<double[]> readCentroids(String filePath) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
                double x = Double.parseDouble(tokenizer.nextToken());
                double y = Double.parseDouble(tokenizer.nextToken());
                centroids.add(new double[]{x, y});
            }
        }
        return centroids;
    }
    private static boolean checkConvergence(List<double[]> oldCentroids, List<double[]> newCentroids, double threshold) {
        if (oldCentroids.size() != newCentroids.size()) return false;

        for (int i = 0; i < oldCentroids.size(); i++) {
            double dx = oldCentroids.get(i)[0] - newCentroids.get(i)[0];
            double dy = oldCentroids.get(i)[1] - newCentroids.get(i)[1];
            double distance = Math.sqrt(dx * dx + dy * dy);

            if (distance > threshold) return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        String inputPath = "/Users/gracerobinson/Project2_BigData/Project2/data_points.txt";
        String outputPath = "/Users/gracerobinson/Project2_BigData/Project2/outputProblem1/step2d";
        String centroidsPath = "/Users/gracerobinson/Project2_BigData/Project2/centroids.txt";

        Configuration conf = new Configuration();
        int maxIterations = 20;
        // This is here for the convergence threshold
        double convergenceThreshold = 0.001;
        int iteration = 0;
        boolean converged = false;

        while (iteration < maxIterations && !converged) {
            System.out.println("Running iteration: " + (iteration + 1));

            // This is here in order to set current centroids as the input
            conf.set("centroids.path", centroidsPath);

            Job job = Job.getInstance(conf, "Optimized K-Means Iteration " + (iteration + 1));
            job.setJarByClass(Step2d.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            Path outputIterationPath = new Path(outputPath + "_iter_" + iteration);
            FileOutputFormat.setOutputPath(job, outputIterationPath);
            job.waitForCompletion(true);
            // This is here to check for convergence stuff
            List<double[]> oldCentroids = readCentroids(centroidsPath);
            List<double[]> newCentroids = readCentroids(outputPath + "_iter_" + iteration + "/part-r-00000");
            converged = checkConvergence(oldCentroids, newCentroids, convergenceThreshold);
            // This is here in order to update the files after each iteration
            centroidsPath = outputPath + "_iter_" + iteration + "/part-r-00000";
            iteration++;
        }

        System.out.println("K-Means completed after " + iteration + " iterations.");
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
