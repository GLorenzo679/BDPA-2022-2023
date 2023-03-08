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

		//BID, title, genre, publisher, yearOfpublication
		inputPath = "data/Books.txt";
		//CustomerId, BID, date, price
		inputPath2 = "data/Purchases.txt";

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

		JavaRDD<String> booksInputRDD = sc.textFile(inputPath);
		JavaRDD<String> purchasesInputRDD = sc.textFile(inputPath2);

		//Select only 2018 purchases
		JavaRDD<String> purchases2018RDD = purchasesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].startsWith("2018/");
			});

		//k: <BID, date>, v: count purchases
		JavaPairRDD<Tuple2<String, String>, Integer> bidDateCountRDD = purchases2018RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(fields[1], fields[2]), 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.cache();

		//k: BID, v: count
		//Select max count purchases
		JavaPairRDD<String, Integer> maxDateBidRDD = bidDateCountRDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._1()._1(), x._2()))
			.reduceByKey((x1, x2) -> {
				if(x1 > x2)
					return x1;
				else
					return x2;
			});

		maxDateBidRDD.saveAsTextFile(outputFolder);
	

		// Close the Spark context
		sc.close();
	}
}
