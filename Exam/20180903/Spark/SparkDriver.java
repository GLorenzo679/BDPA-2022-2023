package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import java.util.ArrayList;

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

		//stockId, date, hour:min, price
		inputPath = "data/Stocks_Prices.txt";


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

		JavaRDD<String> stocksInputRDD = sc.textFile(inputPath);

		//Select only 2017 stocks
		JavaRDD<String> stocks2017RDD = stocksInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[1].startsWith("2017");
			});

		//k: <stockId, date>, v: price
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Double>> stockDatePriceRDD = stocks2017RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				Double price = Double.parseDouble(fields[3]);
				String stockId = fields[0];
				String date = fields[1];

				return new Tuple2<Tuple2<String, String>, Tuple2<Double, Double>>
					(new Tuple2<String, String>(stockId, date),
					new Tuple2<Double, Double>(price, price));
			});

		//k: <stockId, date>, v: <min, max>
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Double>> stockDailyMinMaxRDD = stockDatePriceRDD
			.reduceByKey((x1, x2) -> {
				Double min = Math.min(x1._1(), x2._1());
				Double max = Math.max(x1._2(), x2._2());

				return new Tuple2<Double, Double>(min, max);
			});

		//k: <stockId, date>, v: variation
		JavaPairRDD<Tuple2<String, String>, Double> stockDailyVariationRDD = stockDailyMinMaxRDD
			.mapToPair(x -> new Tuple2<Tuple2<String, String>, Double>
							(new Tuple2<String,String>(x._1()._1(), x._1()._2()), x._2()._2() - x._2()._1()))
			.cache();

		//k: stockId, v: count days variation over 10
		JavaPairRDD<String, Integer> output1 = stockDailyVariationRDD
			.filter(x -> x._2() > 10)
			.mapToPair(x -> new Tuple2<String, Integer>(x._1()._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		output1.saveAsTextFile(outputFolder);

		//[k: <stockId, date>, v: variation; ...]
		JavaPairRDD<Tuple2<String, String>, Double> mapDailyVariationRDD = stockDailyVariationRDD
			.flatMapToPair(x -> {
				String date = x._1()._2();
				Double variation = x._2();
				//String previousDate = DateTool.previousDate(date);

				ArrayList<Tuple2<Tuple2<String, String>, Double>> list = new ArrayList<Tuple2<Tuple2<String, String>, Double>>();

				list.add(x);
				//here date is date - 1
				list.add(new Tuple2<Tuple2<String, String>, Double>(new Tuple2<String, String>(x._1()._1(), date), variation));

				return list.iterator();
			});

		//k: <stockId, date>, v: [variation1, variation2]
		JavaPairRDD<Tuple2<String, String>, Iterable<Double>> groupedVariationRDD = mapDailyVariationRDD
			.groupByKey();

		JavaPairRDD<String, String> output2 = groupedVariationRDD
			.filter(x -> {
				Double var1 = -1.0;
				Double var2 = -1.0;

				for(Double e : x._2()){
					if(var1 < 0)
						var1 = e;
					else
						var2 = e;
				}

				return Math.abs(var1 - var2) <= 0.1 && var2 >= 0;
			})
			.keys()
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()));

		output2.saveAsTextFile(outputFolder2);
	
		// Close the Spark context
		sc.close();
	}
}
