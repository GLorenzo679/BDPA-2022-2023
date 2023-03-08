package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
	
public class SparkDriver {
	
	public static void main(String[] args) {
		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;
		
		inputPath = "data/Reviews.csv";
		outputPath = "data_out";

	
		// Create a configuration object and set the name of the application
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		// ***********************************
		// Task 1
		// ***********************************

		// Map inputRDD to obtain K: UserID, V: ProductID
		// And Remove Header from mapped RDD
		JavaPairRDD <String, String> ReviewsRDD = inputRDD.mapToPair(x -> {
			String [] fields = x.split(",");
			return new Tuple2<String, String>(fields[2], fields[1]);
		});
		
		// Remove Header and duplicate reviews for same product 
		JavaPairRDD <String, String> ReviewsNoHeaderNoDuplicatesRDD = ReviewsRDD.filter(x -> !x._2.contains("Id"))
																				.distinct();

		// Group by UserID
		JavaPairRDD <String, Iterable<String>> ReviewsGroupedRDD = ReviewsNoHeaderNoDuplicatesRDD.groupByKey();

		// Extract the list of reviewed products for each user
		JavaRDD <Iterable<String>> ProductListRDD = ReviewsGroupedRDD.values();

		// Compute Products Pair, K: productId, V: [productId1, productId2, ...]
		JavaPairRDD <String, Integer> ProductPairRDD = ProductListRDD.flatMapToPair(products-> {
			List <Tuple2<String, Integer>> pairs = new ArrayList<> ();

			for(String p1 : products){
				for(String p2 : products){
					if(p1.compareTo(p2) > 0)
						pairs.add(new Tuple2<String,Integer>(p2 + " " + p1, 1));
				}
			}
			
			return pairs.iterator();
		});

		// Sum single occurencies of same pair
		JavaPairRDD <String, Integer> FrequencyPairRDD = ProductPairRDD.foldByKey(0, (x1, x2) -> x1 + x2);

		// Select only pairs that appear more than 1 time
		JavaPairRDD <String, Integer> FrequencyOverThresholdPairRDD =
										FrequencyPairRDD.filter(x -> x._2() > 1);

		// Swap Key Value
		JavaPairRDD <Integer, String> SwappedFrequencyRDD = FrequencyOverThresholdPairRDD.mapToPair(x -> 
			new Tuple2<Integer, String>(x._2(), x._1())
		);

		// Sort by descending order of frequency
		JavaPairRDD <Integer, String> resultRDD = SwappedFrequencyRDD.sortByKey(false);

		// Store the result in the output folder
		resultRDD.saveAsTextFile(outputPath);

		// ***********************************
		// Task 2
		// ***********************************

		System.out.println("Top 10 frequent pairs:");

		List<Tuple2<String, Integer>> top10RDD = FrequencyPairRDD.top(10, new FreqComparator());

		for(Tuple2<String, Integer> x : top10RDD)
			System.out.println("(" + x._1() + "," + x._2() + ")");

		// Close the Spark context
		sc.close();
	}
}
