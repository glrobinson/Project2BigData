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
import java.util.*;

public class Step2e {
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

    // combiner
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
            double sumX = 0, sumY = 0;
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

    // Output Convergence Status
    private static void writeConvergenceStatus(String outputPath, boolean converged) throws IOException {
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath + "/convergence_status.txt"))) {
            writer.write(converged ? "yes - it has converged" : "no - it has not yet converged");
        }
    }

    // helper
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

    // This outputs clustered data that returns the final clustered data points along with their cluster centers!!!
    private static void saveFinalClusteredData(String inputPath, String centroidsPath, String outputPath) throws IOException {
        // Read the final centroids
        List<double[]> centroids = readCentroids(centroidsPath);

        // Create a map to store points for each centroid
        Map<String, List<String>> clusteredData = new HashMap<>();

        // Read data points and assign them to the nearest centroid
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
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

                // Add data point to its centroid's list
                clusteredData.computeIfAbsent(nearestCentroid, k -> new ArrayList<>()).add(x + "," + y);
            }
        }

        // Write clustered data to file
        File outputFile = new File(outputPath + "/final_clustered_data.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (Map.Entry<String, List<String>> entry : clusteredData.entrySet()) {
                writer.write("Centroid: " + entry.getKey() + "\n");
                for (String point : entry.getValue()) {
                    writer.write("  " + point + "\n");
                }
                writer.write("\n"); // Separate clusters
            }
        }
    }

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

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        String inputPath = "/Users/gracerobinson/Project2_BigData/Project2/data_points.txt";
        String outputPath = "/Users/gracerobinson/Project2_BigData/Project2/outputProblem1/step2e";
        String centroidsPath = "/Users/gracerobinson/Project2_BigData/Project2/centroids.txt";

        Configuration conf = new Configuration();
        int maxIterations = 20;
        double convergenceThreshold = 0.001;
        int iteration = 0;
        boolean converged = false;

        while (iteration < maxIterations && !converged) {
            System.out.println("Running iteration: " + (iteration + 1));

            conf.set("centroids.path", centroidsPath);

            Job job = Job.getInstance(conf, "Final K-Means Iteration " + (iteration + 1));
            job.setJarByClass(Step2e.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            Path outputIterationPath = new Path(outputPath + "_iter_" + iteration);
            FileOutputFormat.setOutputPath(job, outputIterationPath);

            job.waitForCompletion(true);

            List<double[]> oldCentroids = readCentroids(centroidsPath);
            List<double[]> newCentroids = readCentroids(outputPath + "_iter_" + iteration + "/part-r-00000");

            converged = checkConvergence(oldCentroids, newCentroids, convergenceThreshold);

            // check each centroid to see which centroid reach convergence
            // after each iteration the convergence statement should be printed out in the console
            int numCentroids = Math.min(oldCentroids.size(), newCentroids.size());

            for (int i = 0; i < numCentroids; i++) {
                double dx = oldCentroids.get(i)[0] - newCentroids.get(i)[0];
                double dy = oldCentroids.get(i)[1] - newCentroids.get(i)[1];
                double distance = Math.sqrt(dx * dx + dy * dy);
                if (distance <= convergenceThreshold) {
                    System.out.println("Centroid " + i + " has converged.");
                } else {
                    System.out.println("Centroid " + i + " has not yet converged.");
                }
            }

            centroidsPath = outputPath + "_iter_" + iteration + "/part-r-00000";

            iteration++;
        }

        System.out.println("K-Means completed after " + iteration + " iterations.");
        writeConvergenceStatus(outputPath, converged);
        saveFinalClusteredData(inputPath, centroidsPath, outputPath);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
