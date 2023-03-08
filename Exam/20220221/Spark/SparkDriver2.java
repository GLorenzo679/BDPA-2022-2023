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

		//ItemID, Name, Category, FirstTimeInCatalog
		inputPath = "data/ItemsCatalog.txt";
		//SaleTimestamp, Username, ItemID, SalePrice
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

		JavaRDD<String> itemInputRDD = sc.textFile(inputPath);
		JavaRDD<String> purchaseInputRDD = sc.textFile(inputPath2);

		//filter 2020 and 2021 purchases
		JavaRDD<String> purchase2021RDD = purchaseInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[0];

				return date.startsWith("2020") || date.startsWith("2021");
			})
			.cache();

		// k: itemID, v: <count 2020, count 2021>
		JavaPairRDD<String, Tuple2<Integer, Integer>> purchaseCountRDD = purchase2021RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String itemID = fields[2];
				String date = fields[0];

				if(date.startsWith("2020"))
					return new Tuple2<String, Tuple2<Integer, Integer>>(itemID, new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<String, Tuple2<Integer, Integer>>(itemID, new Tuple2<Integer, Integer>(0, 1));
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()));

		JavaRDD<String> output1 = purchaseCountRDD
			.filter(x -> x._2()._1() >= 10000 && x._2()._2() >= 10000)
			.keys();

		output1.saveAsTextFile(outputFolder);

		//filter only 2020 purchases
		JavaRDD<String> purchase2020RDD = purchaseInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[0];

				return date.startsWith("2020");
			});

		// k: <itemID, month>, v: username
		//only distinct key-values
		JavaPairRDD<Tuple2<String, Integer>, String> itemMonthUsernameRDD = purchase2020RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String itemID = fields[2];
				Integer month = Integer.parseInt(fields[0].split("/")[1]);
				String username = fields[1]; 

				return new Tuple2<Tuple2<String, Integer>, String>(new Tuple2<String, Integer>(itemID, month), username);
			})
			.distinct();

		// k: itemID v: <month, count distinct customers>
		JavaPairRDD<String, Tuple2<Integer, Integer>> countDistinctUsersRDD = itemMonthUsernameRDD
			.mapToPair(x -> new Tuple2<Tuple2<String, Integer>, Integer>(new Tuple2<String, Integer>(x._1()._1(), x._1()._2()), 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.mapToPair(x -> new Tuple2<String, Tuple2<Integer, Integer>>(x._1()._1(), new Tuple2<Integer, Integer>(x._1()._2(), x._2())));

		// k: itemID v: number of month with less than 10 customers
		JavaPairRDD<String, Integer> less2MonthRDD = countDistinctUsersRDD
			.mapToPair(x -> {
				if(x._2()._2() < 10)
					return new Tuple2<String, Integer>(x._1(), 1);
				else
					return new Tuple2<String, Integer>(x._1(), 0);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() < 2);

		// k: ItemID, v: category
		// select only items first in catalog before 2020
		JavaPairRDD<String, String> before2020ItemsRDD = itemInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[3].split("-")[0];

				return date.compareTo("2020/01/01") < 0;
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});

		// k: itemID, v: category
		JavaPairRDD<String, String> output2 = before2020ItemsRDD
			.subtractByKey(less2MonthRDD);

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
