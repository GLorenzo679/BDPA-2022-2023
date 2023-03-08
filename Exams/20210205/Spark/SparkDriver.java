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

		//PlantID, City, Country
		inputPath = "data/ProductionPlants.txt";
		//RID, PlantID, IP
		inputPath2 = "data/Robots.txt";
		//RID, FailureTypeCode, Date, Time
		inputPath3 = "data/Failures.txt";

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

		JavaRDD<String> plantsInputRDD = sc.textFile(inputPath);
		JavaRDD<String> robotsInputRDD = sc.textFile(inputPath2);
		JavaRDD<String> failuresInputRDD = sc.textFile(inputPath3);

		JavaRDD<String> failures2020RDD = failuresInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].startsWith("2020/");
			});

		//k: RID, v: countFailures
		JavaPairRDD<String, Integer> numFailuresRDD = failures2020RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String RID = fields[0];

				return new Tuple2<String, Integer>(RID, 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.cache();

		//k: RID, v: PlantID
		JavaPairRDD<String, String> plantRobotIdRDD = robotsInputRDD
		.mapToPair(x -> {
			String[] fields = x.split(",");
			String RID = fields[0];
			String PlantID = fields[1];

			return new Tuple2<String, String>(RID, PlantID);
		});

		//k: RID, v: <PlantID, count>
		JavaPairRDD<String, Tuple2<String, Integer>> joinedRDD = plantRobotIdRDD
			.join(numFailuresRDD)
			.cache();

		//select only pairs with at lest 50 failures
		//then take PlaintID
		JavaRDD<String> output1 = joinedRDD
			.filter(x -> x._2()._2() >= 50)
			.map(x -> x._2()._1())
			.distinct();
			
		output1.saveAsTextFile(outputFolder);

		//k: PlantID, v: count of faulty robots
		JavaPairRDD<String, Integer> numFailedRobotPerPlantRDD = joinedRDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._2()._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.cache();

		//k: PlantID, v: 0
		JavaPairRDD<String, Integer> plantIDZeroRDD = plantsInputRDD
			.mapToPair(x -> {
				String[] fields = x.split("");

				return new Tuple2<String, Integer>(fields[0], 0);
			});

		// Select only the pairs with 0 faulty robots
		JavaPairRDD<String, Integer> onlyPlantIDZeroRDD = plantIDZeroRDD
			.subtractByKey(numFailedRobotPerPlantRDD);

		// Union on plants with 0 and more faulty robots
		JavaPairRDD<String, Integer> output2 = onlyPlantIDZeroRDD
			.union(numFailedRobotPerPlantRDD);

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
