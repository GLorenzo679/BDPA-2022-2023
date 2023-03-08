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

		//PID, Date, ApplicationName, BriefDescription
		inputPath = "data/Patches.txt";

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

		JavaRDD<String> patchesInputRDD = sc.textFile(inputPath);

		//Select only 2017 patches of windows 10 and ubuntu 18.04 applications
		JavaRDD<String> patches2017RDD = patchesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[1].startsWith("2017")
					&& (fields[2].equals("Windows 10") || fields[2].equals("Ubuntu 18.04"));
			});

		//k: month, v: <count w10, count u18>
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> monthApllicationCountRDD = patches2017RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				Integer month = Integer.parseInt(fields[1].split("/")[1]);
				String applicationName = fields[2];

				if(applicationName.equals("Windows 10"))
					return new Tuple2<Integer, Tuple2<Integer, Integer>>(month, new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<Integer, Tuple2<Integer, Integer>>(month, new Tuple2<Integer, Integer>(0, 1));
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()));

		//k: month, v: W/U
		//filter out months where count w10 == count u18
		JavaPairRDD<Integer, String> output1 = monthApllicationCountRDD
			.filter(x -> x._2()._1() != x._2()._2())
			.mapToPair(x -> {
				if(x._2()._1() > x._2()._2())
					return new Tuple2<Integer, String>(x._1(), "W");
				else
					return new Tuple2<Integer, String>(x._1(), "U");
			});

		output1.saveAsTextFile(outputFolder);

		//Select only 2018 patches
		JavaRDD<String> patches2018RDD = patchesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[1].startsWith("2018");
			});

		




		// Close the Spark context
		sc.close();
	}
}
