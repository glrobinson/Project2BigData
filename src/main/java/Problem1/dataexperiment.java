package Problem1;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class dataexperiment {
    private static final int NUM_POINTS = 3000; // Number of data points
    private static final int MAX_XY_DATA = 5000; // Max value for (x, y) in data points
    private static final int MAX_XY_CENTROID = 10000; // Max value for (x, y) in centroids

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Ask for the number of centroids (K)
        System.out.print("Enter the number of centroids (K): ");
        int K = scanner.nextInt();
        scanner.close();

        // Generate the large dataset of points
        generateDataPoints("data_points2.txt");

        // Generate K initial centroids
        generateCentroids("centroids2.txt", K);

        System.out.println("Data generation complete! Files created:");
        System.out.println("- data_points.txt (3,000+ random (x,y) points)");
        System.out.println("- centroids.txt (" + K + " initial centroids)");
    }

    /**
     * Generates a file with 3,000+ random (x, y) data points.
     */
    public static void generateDataPoints(String filename) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter(filename)) {
            for (int i = 0; i < NUM_POINTS; i++) {
                int x = random.nextInt(MAX_XY_DATA + 1); // Random x between 0 and 5000
                int y = random.nextInt(MAX_XY_DATA + 1); // Random y between 0 and 5000
                writer.write(x + "," + y + "\n");
            }
            System.out.println("Generated " + NUM_POINTS + " data points in " + filename);
        } catch (IOException e) {
            System.err.println("Error writing to " + filename + ": " + e.getMessage());
        }
    }

    /**
     * Generates a file with K random (x, y) centroids.
     */
    public static void generateCentroids(String filename, int K) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter(filename)) {
            for (int i = 0; i < K; i++) {
                int x = random.nextInt(MAX_XY_CENTROID + 1); // Random x between 0 and 10000
                int y = random.nextInt(MAX_XY_CENTROID + 1); // Random y between 0 and 10000
                writer.write(x + "," + y + "\n");
            }
            System.out.println("Generated " + K + " initial centroids in " + filename);
        } catch (IOException e) {
            System.err.println("Error writing to " + filename + ": " + e.getMessage());
        }
    }
}
