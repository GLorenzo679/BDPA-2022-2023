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

		//SID, IP, DataCenterID
		inputPath = "data/Servers.txt";
		//Date, Time, SID, FailureType, Downtime
		inputPath2 = "data/Failures.txt";


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

		JavaRDD<String> serversInputRDD = sc.textFile(inputPath);
		JavaRDD<String> failuresInputRDD = sc.textFile(inputPath2);

		//Select only 2017 failures
		JavaRDD<String> failures2017RDD = failuresInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[0].startsWith("2017");
			})
			.cache();

		//k: SID, v: DatacenterId
		JavaPairRDD<String, String> SIDDatacenterRDD = serversInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[2]);
			});

		//k: SID, v: count failure server
		JavaPairRDD<String, Integer> SIDOnesRDD = failures2017RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[2], 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: DatacenterId, v: count failures datacenter
		JavaPairRDD<String, Integer> datacentersFailuresCountRDD = SIDDatacenterRDD
			.join(SIDOnesRDD)
			.mapToPair(x -> new Tuple2<String, Integer>(x._2()._1(), x._2()._2()))
			.reduceByKey((x1, x2) -> x1 + x2);

		//Select only datacenters with at least 365 failures
		JavaPairRDD<String, Integer> output1 = datacentersFailuresCountRDD
			.filter(x -> x._2() >= 365);

		output1.saveAsTextFile(outputFolder);

		//k: SID, v: total downtime
		//Select only servers with more than 1440 minutes of downtime
		JavaPairRDD<String, Integer> sidDowntimeRDD = failures2017RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[2], Integer.parseInt(fields[4]));
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() > 1440);

		//k: <SID, month>, v: num failures
		//Select only month with at least 2 failures
		JavaPairRDD<Tuple2<String, Integer>, Integer> sidMonthFailuresCountOver2RDD = failures2017RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				Integer month = Integer.parseInt(fields[0].split("/")[1]);
				String SID = fields[2];

				return new Tuple2<Tuple2<String, Integer>, Integer>(new Tuple2<String, Integer>(SID, month), 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() >= 2);

		//k: SID, v: count of months with at least 2 failures
		//Select only sid with 12 differents months of at least 2 failures 
		JavaPairRDD<String, Integer> numMonthFailuresRDD = sidMonthFailuresCountOver2RDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._1()._1() , 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() == 12);

		JavaRDD<String> output2 = numMonthFailuresRDD	
			.join(sidDowntimeRDD)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
