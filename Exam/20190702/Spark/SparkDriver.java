package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		String inputPath3;
		String outputFolder;
		String outputFolder2;

		//BID, BicycleManufacturer, City, Country
		inputPath = "data/Bicycles.txt";
		//Timestamp, BID, Component
		inputPath2 = "data/Bicycles_Failures.txt";

		outputFolder = "data_out_1";
		outputFolder2 = "data_out_2";

		//inputPath = args[0];
		//inputPath2 = args[1];
		//inputPath3 = args[3];
		//outputFolder = args[4];
		//outputFolder2 = args[5];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark_Template").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> bicylesInputRDD = sc.textFile(inputPath);
		JavaRDD<String> failuresInputRDD = sc.textFile(inputPath2);

		JavaRDD<String> failures2018RDD = failuresInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[0].startsWith("2018");
			})
			.cache();

		//k: <BID, month>, v: count of failures
		JavaPairRDD<Tuple2<String, String>, Integer> bidMonthFailuresRDD = failures2018RDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].equals("wheel");
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String month = fields[0].split("/")[1];

				return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(fields[1], month), 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: BID, v: count month failures
		JavaPairRDD<String, Tuple2<String, Integer>> bidNumMonthFailuresRDD = bidMonthFailuresRDD
			.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._1()._1(), new Tuple2<String, Integer>(x._1()._2(), x._2())))
			.filter(x -> x._2()._2() > 2);

		JavaRDD<String> output1 = bidNumMonthFailuresRDD
			.keys()
			.distinct();

		output1.saveAsTextFile(outputFolder);
		
		//k: BID, v: count failures
		JavaPairRDD<String, Integer> bidCountFailuresRDD= failures2018RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[1], 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		//Select the bicyles with over 20 failures in 2018
		JavaPairRDD<String, Integer> bidOver20FailuresRDD = bidCountFailuresRDD
			.filter(x -> x._2() > 20);

		//k: BID, v: city
		JavaPairRDD<String, String> bidCityRDD = bicylesInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});

		//k: city, v: num bicycles with < 20 failures
		//Remove the bicycles with over 20 failures -> keep bicycles with 0 or < 20 failures
		JavaPairRDD<String, Integer> numSelectedBidCityRDD = bidCityRDD
			.subtractByKey(bidOver20FailuresRDD)
			.mapToPair(x -> new Tuple2<String, Integer>(x._2(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: city, v: num all bicycles
		JavaPairRDD<String, Integer> numBidCityRDD = bidCityRDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._2(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		//if num all bicycles == num bicycles with < 20 failures ->
		//all bicyles of that city are good -> take city
		JavaRDD<String> output2 = numBidCityRDD
			.join(numSelectedBidCityRDD)
			.filter(x -> x._2()._1() == x._2()._2())
			.keys()
			.cache();

		output2.saveAsTextFile(outputFolder2);

		System.out.println(output2.count());

		// Close the Spark context
		sc.close();
	}
}
