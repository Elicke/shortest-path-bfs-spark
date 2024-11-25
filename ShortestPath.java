import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.ArrayList;
import scala.Tuple2;
import java.util.Map;
import java.util.HashMap;

public class ShortestPath {

    public static void main(String[] args) {

        // only output any error logs
        Logger.getLogger("org").setLevel(Level.ERROR);

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

        // while there are any "ACTIVE" vertices
        while (network.filter( vertex -> vertex._2().status.equals("ACTIVE") ).count() > 0) {

            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair( vertex -> {
                List<Tuple2<String, Data>> list = new ArrayList<>();

                // if the vertex is "ACTIVE", create neighbors with new distances
                if (vertex._2().status.equals("ACTIVE")) {
                    for (Tuple2<String, Integer> neighbor : vertex._2().neighbors) {
                        Integer newDistance = vertex._2().distance + neighbor._2(); // newDistance = current vertex's accumulated distance + link weight traveling to neighbor
                        list.add(new Tuple2<>(neighbor._1(), new Data(new ArrayList<>(), newDistance, null, null)));
                    }
                }

                // add this vertex itself to the list as well
                list.add(new Tuple2<>(vertex._1(), new Data(vertex._2().neighbors, vertex._2().distance, vertex._2().prev, "INACTIVE")));
                return list.iterator();
            } );

            network = propagatedNetwork.reduceByKey( (v1, v2) -> {
                ArrayList<Tuple2<String, Integer>> neighborsMerged;
                Integer shorterDistance;
                Integer prevMerged;

                if (v1.neighbors.isEmpty()) {
                    neighborsMerged = new ArrayList<>(v2.neighbors);
                } else {
                    neighborsMerged = new ArrayList<>(v1.neighbors);
                }

                if (v1.distance.equals(0) || (!v2.distance.equals(0) && v2.distance.compareTo(v1.distance) <= 0)) {
                    shorterDistance = v2.distance;
                } else {
                    shorterDistance = v1.distance;
                }

                if (v1.prev == null) {
                    prevMerged = v2.prev;
                } else {
                    prevMerged = v1.prev;
                }

                return new Data(neighborsMerged, shorterDistance, prevMerged, "INACTIVE");
            } );

            network = network.mapValues( value -> {
                Integer newPrev;
                String newStatus;

                if (value.distance.equals(0) || (value.prev != null && value.prev.compareTo(value.distance) <= 0)) {
                    newPrev = value.prev;
                    newStatus = "INACTIVE";
                } else {
                    newPrev = value.distance;
                    newStatus = "ACTIVE";
                }

                return new Data(value.neighbors, value.distance, newPrev, newStatus);
            } );

        }

        long stopTime = System.currentTimeMillis();
        long executionTime = stopTime - startTime;

        Map<String, Data> map = network.collectAsMap();
        HashMap<String, Data> hmap = new HashMap<String, Data>(map);

        System.out.println("from " + args[1] + " to " + args[2] + " takes distance = " + hmap.get(args[2]).distance);
        System.out.println("" + executionTime + " milliseconds");

    }

}
