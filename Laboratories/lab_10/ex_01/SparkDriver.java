package it.polito.bigdata.spark;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.List;
import java.util.ArrayList;
import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];
	
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Streaming Lab 10");
				
		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));		
		
		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");

		JavaDStream<String> tweets = jssc.textFileStream(inputFolder);

		// Process the tweets JavaDStream.
		// Every time a new file is uploaded  in inputFolder a new batch of streaming data 
		// is generated 
		JavaDStream<String>	hashtag = tweets.flatMap(x -> {
			String[] fields = x.split("\t");

			//26976263 Gym time!!!! #fitness #prova1 dsadsadsa #prova2
			String[] hashtagList = fields[1].split("\\s+");
			List<String> output = new ArrayList<>();

			for(int i = 0; i < hashtagList.length; i++)
				if(hashtagList[i].startsWith("#"))
					output.add(hashtagList[i]);

			return output.iterator();
		});

		JavaPairDStream<String, Integer> hashtagOnes = hashtag.mapToPair(x -> {
			return new Tuple2<String, Integer>(x.toLowerCase(), +1);
		});

		JavaPairDStream<String, Integer> hashtagFreq = hashtagOnes.reduceByKeyAndWindow((Integer x1, Integer x2) -> {
			return x1 + x2;
		}, Durations.seconds(30), Durations.seconds(10));

		JavaPairDStream<Integer, String> hashtagFreqSorted = hashtagFreq.transformToPair((JavaPairRDD<String, Integer> rdd) -> {
			// Swap keys with values and then sort the RDD by key
			JavaPairRDD<Integer, String> swapRDD = rdd.mapToPair((Tuple2<String, Integer> element) -> {
				return new Tuple2<Integer, String>(element._2(), element._1());
			});

			return swapRDD.sortByKey(false);
		});
		
		hashtagFreq.print();
		hashtagFreq.dstream().saveAsTextFiles(outputPathPrefix, "");

		// Start the computation
		jssc.start();              
		
		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);
		
		jssc.close();
		
	}
}
