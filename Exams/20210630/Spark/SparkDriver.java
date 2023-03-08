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
		String outputFolder;
		String outputFolder2;

		//ModelID, MName, Manifacturer
		inputPath = "data/BikeModels.txt";
		//SID, BikeID, ModelID, Date, Country, Price, EU
		inputPath2 = "data/Sales.txt";
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

		JavaRDD<String> salesInputRDD = sc.textFile(inputPath);
		JavaRDD<String> modelsInputRDD = sc.textFile(inputPath2);

		//k: modelID, v: <price, price>
		//selects only pairs in year 2020 sold in EU
		JavaPairRDD<String, Tuple2<Double, Double>> filteredSalesRDD = salesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				String EU = fields[6];
				String year = fields[3].split("/")[0];

				return EU.equals("T") && year.equals("2020");
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				String modelID = fields[2];
				Double price = Double.parseDouble(fields[5]);

				return new Tuple2<String, Tuple2<Double, Double>>(modelID, new Tuple2<Double, Double>(price, price));
			});

		//k: modelID, v: <min, max>
		//compute min and max for each modelID
		JavaPairRDD<String, Tuple2<Double, Double>> minMaxSalesRDD = filteredSalesRDD
			.reduceByKey((x1, x2) -> {
				Double min;
				Double max;

				if(x1._1() < x2._1())
					min = x1._1();
				else
					min  = x2._1();

				if(x1._2() > x2._2())
					max = x1._2();
				else
					max  = x2._2();

				return new Tuple2<Double, Double>(min, max);

				/*
					ALTERNATIVE SOLUTION:
					Double min = Math.min(i1._1(), i2._1());
        			Double max = Math.max(i1._2(), i2._2());

        			return new Tuple2<Double, Double>(min, max);
				*/
			});

		//Select only models with variation > 5000
		JavaRDD<String> output1 = minMaxSalesRDD
			.filter(x -> x._2()._2() - x._2()._1() > 5000)
			.keys();

		output1.saveAsTextFile(outputFolder);

		JavaPairRDD<String, String> modelManufacturerRDD = modelsInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});

		//Select modelID of models sold more than 10 times
		JavaPairRDD<String, Integer> frequentModelsRDD = salesInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				String modelID = fields[2];

				return new Tuple2<String, Integer>(modelID, 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() > 10);

		//k: manufacturer, v: count
		//Eliminates models that have been sold more than 10 times (frequent ones)
		JavaPairRDD<String, Integer> notFrequentModelsRDD = modelManufacturerRDD
			.subtractByKey(frequentModelsRDD)
			.mapToPair(x -> new Tuple2<String, Integer>(x._2(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		//Select only manufacturers with at least 15 unfrequent models
		JavaPairRDD<String, Integer> output2 =  notFrequentModelsRDD
			.filter(x -> x._2() >= 15);

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
