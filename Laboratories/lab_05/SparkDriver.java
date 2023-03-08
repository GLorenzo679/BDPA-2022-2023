package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.*;
import scala.Tuple2;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath = "test";
		outputPath = "test_out";
		prefix = "ho";

	
		// Create a configuration object and set the name of the application
		//SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		// Task 1
		JavaRDD<String> filteredRDD = wordFreqRDD.filter(x -> x.startsWith(prefix));

		// Print the number of filtered lines
		System.out.println("Number of filtered lines: " + filteredRDD.count());

		JavaPairRDD <String, Integer> WordFreqFilteredRDD = filteredRDD.mapToPair(x -> {
			String[] fields = x.split("\\s+");
			return new Tuple2<String, Integer>(fields[0], Integer.parseInt(fields[1]));
		});

		Integer maxfreq = WordFreqFilteredRDD.values().reduce((x1, x2) -> Math.max(x1, x2));
		System.out.println("Max frequency: " + maxfreq);
		
		// Task 2
		Double threshold = 0.8 * maxfreq;

		// Filter lines with freq < threshold
		JavaPairRDD <String, Integer> ThresholdFilteredRDD = WordFreqFilteredRDD.filter(x -> x._2() > threshold);

		// Print the number of filtered lines
		System.out.println("Number of filtered lines: " + ThresholdFilteredRDD.count());

		ThresholdFilteredRDD.keys().saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
