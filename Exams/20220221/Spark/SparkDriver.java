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

		inputPath = "data/ItemsCatalog.txt";
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

		JavaRDD<String> ItemsCatalogInputRDD = sc.textFile(inputPath);
		JavaRDD<String> PurchasesInputRDD = sc.textFile(inputPath2);

		JavaRDD<String> Purchases20_21RDD = PurchasesInputRDD
			.filter(x -> (x.startsWith("2020/") || x.startsWith("2021/")))
			.cache();

		//k: ItemID, v: <?2020, ?2021>
		JavaPairRDD<String, Tuple2<Integer, Integer>> PurchasesOnes20_21RDD = Purchases20_21RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String year = fields[0].split("/")[0];

				if(year.equals("2020"))
					return new Tuple2<String, Tuple2<Integer, Integer>>(fields[2], new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<String, Tuple2<Integer, Integer>>(fields[2], new Tuple2<Integer, Integer>(0, 1));
			});

		//k: ItemID, v: <count_2020, count_2021>
		JavaPairRDD<String, Tuple2<Integer, Integer>> PurchasesCount20_21RDD = PurchasesOnes20_21RDD
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()));

		JavaRDD<String> output1RDD = PurchasesCount20_21RDD
			.filter(x -> (x._2()._1() >= 10000 && x._2()._2() >= 10000))
			.keys();

		output1RDD.saveAsTextFile(outputFolder);

		//k: ItemID, v: Category
		//Select only items in catalog before 2020
		JavaPairRDD<String, String> itemCategoryRDD = ItemsCatalogInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				if(fields[3].compareTo("2020/01/01") < 0)
					return true;
				else
					return false;
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});

		JavaRDD<String> Purchase2020RDD = Purchases20_21RDD
			.filter(x -> x.startsWith("2020/"));

		//k: <ItemID, month>, v: username
		JavaPairRDD<Tuple2<String, String>, String> distinctItemMonthRDD = Purchase2020RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String month = fields[0].split("/")[1];

				return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(fields[2], month), fields[1]);
			})
			.distinct();

		//k: <ItemID, month>, v: count user
		//select only item, month pairs whit more than 9 distinct costumers
		JavaPairRDD<Tuple2<String, String>, Integer> moreThan9CostumersRDD = distinctItemMonthRDD
			.mapToPair(x -> {
				return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(x._1()._1(), x._1()._2()), 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() >= 10);

		//k: itemID, v: count month with more than 9 distinct costumers
		JavaPairRDD<String, Integer> numMonthsRDD = moreThan9CostumersRDD
			.mapToPair(x -> new Tuple2<>(x._1()._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);
		
		//Select only the items with more than 10 months with  more than 9 distinct costumers
		JavaPairRDD<String, Integer> tmp = numMonthsRDD
			.filter(x -> (x._2() > 10));

		//Subtract the keys of the "good" items
		JavaPairRDD<String, String> output2 = itemCategoryRDD.subtractByKey(tmp);

		output2.saveAsTextFile(outputFolder2);


		// Close the Spark context
		sc.close();
	}
}
