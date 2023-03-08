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

		//SID, OperatingSystem, Model
		inputPath = "data/Servers.txt";
		//PID, ReleaseDate, OperatingSystem
		inputPath2 = "data/Pathches.txt";
		//PID, SID, ApplicationDate
		inputPath3 = "data/AppliedPatches.txt";

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

		JavaRDD<String> serverInputRDD = sc.textFile(inputPath);
		JavaRDD<String> patchesInputRDD = sc.textFile(inputPath2);
		JavaRDD<String> apInputRDD = sc.textFile(inputPath3);

		// k: PID, v: release date
		JavaPairRDD<String, String> patchDateRDD = patchesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].equals("Ubuntu 2");
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[1]);
			});

		// k: PID, v: application date
		JavaPairRDD<String, String> apDateRDD = apInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});
		
		// k: PID, v: count servers
		JavaPairRDD<String, Integer> countSameDateRDD = apDateRDD
			.join(patchDateRDD)
			.filter(x -> x._2()._1().equals(x._2()._2()))
			.mapToPair(x -> new Tuple2<String, Integer>(x._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		JavaRDD<String> output1 = countSameDateRDD
			.filter(x -> x._2() > 100)
			.keys();

		output1.saveAsTextFile(outputFolder);

		// filter only 2021 tuples
		JavaRDD<String> patches2021RDD = apInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].startsWith("2021");
			});

		// k: <SID, month>, v: count
		JavaPairRDD<String, Integer> sidMonthCOuntRDD = patches2021RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				Integer month = Integer.parseInt(fields[2].split("/")[1]);

				return new Tuple2<String, Integer>(fields[1], month);
			})
			.distinct();

		// k: SID, v: count of different months without patches
		JavaPairRDD<String, Integer> sidNumMonthRDD = sidMonthCOuntRDD
			.groupByKey()
			.mapValues(x -> {
				Integer num = 0;

				for(Integer e : x)
					num++;

				return 12 - num;
			});

		// k: SID, v: 0
		JavaPairRDD<String, Integer> serversNoPatchRDD = serverInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[0], 12);
			});

		// use a leftOuterJoin to join missingServers with missingMonthsPerServer
		// key = SID
		// value = (12, Optional(missingMonths))
		// if missingMonths value is present, keep that, otherwise substitute it with 12
		JavaPairRDD<String, Integer> output2 = serversNoPatchRDD
							.leftOuterJoin(sidNumMonthRDD)
							.mapValues(x -> x._2().isPresent() ? x._2().get() : x._1());

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
