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

		//SID, Model
		inputPath = "data/Servers.txt";
		//SID, PID, Date
		inputPath2 = "data/PatchedServers.txt";

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
		JavaRDD<String> patchesInputRDD = sc.textFile(inputPath2);

		//Select only 2018 or 2019 patches
		JavaRDD<String> patches1819RDD = patchesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String year = fields[2].split("/")[0];

				return (year.equals("2018") || year.equals("2019"));
			});

		//k: SID, v: model
		JavaPairRDD<String, String> sidModelRDD = serversInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[1]);
			})
			.cache();

		//k: <SID, year>, v: 1
		JavaPairRDD<Tuple2<String, String>, Integer> sidYearOnesRDD = patches1819RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String year = fields[2].split("/")[0];

				return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(fields[0], year), 1);
			});

		//k: <SID, year>, v: count
		JavaPairRDD<Tuple2<String, String>, Integer> sidYearCountRDD =  sidYearOnesRDD
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: SID, v: <count2018, count2019>
		JavaPairRDD<String, Tuple2<Integer, Integer>> sid1819CountRDD = sidYearCountRDD
			.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._1()._1(), new Tuple2<String, Integer>(x._1()._2(), x._2())))
			.groupByKey()
			.mapValues(x -> {
				Integer count2018 = 0;
				Integer count2019 = 0;

				for(Tuple2<String, Integer> e : x){
					if(e._1().equals("2018"))
						count2018 = e._2();
					else
						count2019 = e._2();
				}

				return new Tuple2<Integer, Integer>(count2018, count2019);
			});

		//Select only sid where count2019 < 0.5 * count2018
		JavaPairRDD<String, Tuple2<Integer, Integer>> sidHighDecreaseRDD = sid1819CountRDD
			.filter(x -> x._2()._2() < 0.5 * x._2()._1());

		JavaPairRDD<String, String> output1 = sidModelRDD
			.join(sidHighDecreaseRDD)
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()._1()));

		output1.saveAsTextFile(outputFolder);

		//k: <SID, date>, v: count_of_patches_in_date
		JavaPairRDD<Tuple2<String, String>, Integer> sidDateCountRDD = patchesInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(fields[0], fields[2]), 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		//Select only SID of servers with 1 patch in a date
		JavaRDD<String> sidOnePatchRDD = sidDateCountRDD
			.filter(x -> x._2() == 1)
			.map(x -> x._1()._1());

		//Select distinct SID of patched servers
		JavaRDD<String> patchedRDD = patchesInputRDD
			.map(x -> {
				String[] fields = x.split(",");

				return fields[0];
			})
			.distinct();

		//Select SID of servers with 0 parches (not in patches file)
		JavaRDD<String> sidNoPatchesRDD = sidModelRDD
			.keys()
			.subtract(patchedRDD);

		//k: SID, v: ""(placeholder)
		//Select sid with 0 or 1 patches in a single date
		JavaPairRDD<String, String> sid01PatchesRDD = sidNoPatchesRDD
			.union(sidOnePatchRDD)
			.mapToPair(x -> new Tuple2<String, String>(x, ""));

		JavaPairRDD<String, String> output2 = sid01PatchesRDD
			.join(sidModelRDD)
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()._2()));

		output2.saveAsTextFile(outputFolder2);

		System.out.println("Number of distinct models of selected servers: " + output2.values().distinct().count());

		// Close the Spark context
		sc.close();
	}
}
