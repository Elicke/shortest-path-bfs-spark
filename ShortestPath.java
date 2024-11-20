import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.ArrayList;
import scala.Tuple2;

public class ShortestPath {
    public static void main(String[] args) {

        // start Spark and read a given input file
        String inputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("BFS-based Shortest Path Search");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile(inputFile);

        // now start a timer
        long startTime = System.currentTimeMillis();

        // String = vertex id
        // Data = vertex attributes (neighbors, status, distance, prev)
        JavaPairRDD<String, Data> network = lines.mapToPair( line -> {
            ArrayList<Tuple2<String, Integer>> neighbors = new ArrayList<>();
            String status;

            String[] substrings = line.split("=");
            String vertexID = substrings[0];
            String neighborsString = substrings[1];
            String[] neighborsArray = neighborsString.split(";");

            for (String n : neighborsArray) {
                String neighborID = n.split(",")[0];
                Integer linkWeight = Integer.valueOf(n.split(",")[1]);
                neighbors.add(new Tuple2<>(neighborID, linkWeight));
            }

            if (vertexID.equals(args[1])) {
                status = "ACTIVE"; // source vertex
            } else {
                status = "INACTIVE"; // all other vertices
            }

            return new Tuple2<>(vertexID, new Data(neighbors, 0, null, status));
        } );

    }
