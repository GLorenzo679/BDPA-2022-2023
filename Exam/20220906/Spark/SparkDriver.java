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
		String outputFolder;
		String outputFolder2;

		//CodDC, CodC, City, Country, Continent
		inputPath = "data/DataCenters.txt";
		//CodDC, Date, kWh
		inputPath2 = "data/DailyPowerConsumption.txt";

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

		JavaRDD<String> consumptionInputRDD = sc.textFile(inputPath2).cache();
		JavaRDD<String> dcInputRDD = sc.textFile(inputPath).cache();

		// k: date, v: consumption
		JavaPairRDD<String, Integer> dcConsumptionRDD = consumptionInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[1], Integer.parseInt(fields[2]));
			});

		// k: date, v: <count consumption >= 1000, count dc>
		JavaPairRDD<String, Tuple2<Integer, Integer>> dateCountRDD = dcConsumptionRDD
			.mapValues(x -> {
				if(x >= 1000)
					return new Tuple2<Integer, Integer>(1, 1);
				else
					return new Tuple2<Integer, Integer>(0, 1);
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()));

		JavaRDD<String> output1 = dateCountRDD
			.filter(x -> x._2()._1() >= 0.9 * x._2()._2())
			.keys();

		output1.saveAsTextFile(outputFolder);

		// k: codDC, v: consumption
		// select only consumption of 2021
		JavaPairRDD<String, Integer> dcConsumption2021RDD = consumptionInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[1].startsWith("2021");
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[0], Integer.parseInt(fields[2]));
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		// k: continent, v: num_dc
		JavaPairRDD<String, Integer> numDcContinentRDD = dcInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[4], 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		// k: codDC, v: continent
		JavaPairRDD<String, String> dcContinentRDD = dcInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[4]);
			});

		// k: continent, v: tot consumption
		JavaPairRDD<String, Integer> continentTotConsumptionRDD = dcContinentRDD
			.join(dcConsumption2021RDD)
			.mapToPair(x -> new Tuple2<String, Integer>(x._2()._1(), x._2()._2()))
			.reduceByKey((x1, x2) -> x1 + x2);

		// k: continent, v: <avg consumption, numDc>
		JavaPairRDD<String, Tuple2<Double, Integer>> continentAvgConsumptionRDD = continentTotConsumptionRDD
			.join(numDcContinentRDD)
			.mapValues(x -> new Tuple2<Double, Integer>(new Double(x._1()/x._2()), x._2()));

		Tuple2<Double, Integer> maxCondition = continentAvgConsumptionRDD
			.values()
			.reduce((x1, x2) -> {
				Double avgConsMax = Math.max(x1._1(), x2._1());
				Integer numDcMax = Math.max(x1._2(), x2._2());

				return new Tuple2<Double, Integer>(avgConsMax, numDcMax);
			});

		// filter only tuples that satisfies max condition
		JavaRDD<String> output2 = continentAvgConsumptionRDD
			.filter(x -> x._2()._1() == maxCondition._1() && x._2()._2() == maxCondition._2())
			.keys();
			
		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
