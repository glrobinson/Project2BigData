package Problem1;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class dataexperiment {
    private static final int NumPoints = 3000;
    private static final int MaxXYData = 5000;
    private static final int MaxXYCentroid = 10000;

    public static void generateDataPoints(String filename) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter(filename)) {
            for (int i = 0; i < NumPoints; i++) {
                int x = random.nextInt(MaxXYData + 1);
                int y = random.nextInt(MaxXYData + 1);
                writer.write(x + "," + y + "\n");
            }
            System.out.println("Yay!!! Generated " + NumPoints + " data points in " + filename);
        } catch (IOException e) {}
    }
    public static void generateCentroids(String filename, int K) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter(filename)) {
            for (int i = 0; i < K; i++) {
                int x = random.nextInt(MaxXYCentroid + 1);
                int y = random.nextInt(MaxXYCentroid + 1);
                writer.write(x + "," + y + "\n");
            }
            System.out.println("Yay!!! Generated " + K + " initial centroids in " + filename);
        } catch (IOException e) {}
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // What is going to be K!!!
        System.out.print("Enter the number of centroids (K): ");
        int K = scanner.nextInt();
        scanner.close();
        generateDataPoints("data_points2.txt");
        generateCentroids("centroids2.txt", K);
    }
}
