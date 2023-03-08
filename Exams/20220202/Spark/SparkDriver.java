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

		inputPath = "data/Apps.txt";
		inputPath2 = "data/Actions.txt";
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

		JavaRDD<String> AppInputRDD = sc.textFile(inputPath);
		JavaRDD<String> ActionsInputRDD = sc.textFile(inputPath2);

		JavaPairRDD<String, String> AppRDD = AppInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[1]);
			});

		//k: <AppId, month>, v: <Install_count, Remove_count>
		JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> ActionsRDD = ActionsInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return (!(fields[3].equals("Download")) && fields[2].startsWith("2021/"));
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String month = fields[2].split("/")[1];

				if(fields[3] == "Install")
					return new Tuple2<>(new Tuple2<String, String>(fields[1], month), new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<>(new Tuple2<String, String>(fields[1], month), new Tuple2<Integer, Integer>(0, 1));
			});

		//k: AppId, v: month
		JavaPairRDD<String, String> moreInstallsRDD = ActionsRDD
			.reduceByKey((x1, x2) -> {
				return new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2());
			})
			.filter(x -> x._2()._1() > x._2()._2())
			.mapToPair(x -> {
				return new Tuple2<String, String>(x._1()._1(), x._1()._2());
			});

		//k: AppId, v: count month installs over remove
		JavaPairRDD<String, Integer> all12MoreInstallRDD= moreInstallsRDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.filter(x -> x._2() == 12);

		JavaPairRDD<String, String> output1 = AppRDD
			.join(all12MoreInstallRDD)
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()._1()));

		output1.saveAsTextFile(outputFolder);

		JavaRDD<String> installedRDD = ActionsInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[3].equals("Install");
			})
			.cache();

		//k: AppId, v: UserId
		//Select users that installed an app before 2022
		JavaPairRDD<String, String> installedBefore2022RDD = installedRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return (fields[2].compareTo("2022/01/01") < 0);
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[1], fields[0]);
			});

		//k: AppId, v: UserId
		//Select only distinct users that installed an app in 2022
		//Does not select users that installed app before 2022, then installed again in 2022
		JavaPairRDD<String, String> installedAfter2022RDD = installedRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return (fields[2].compareTo("2022/01/01") >= 0);
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[1], fields[0]);
			})
			.distinct()
			.subtractByKey(installedBefore2022RDD);

		//k: AppId, v: count Install
		JavaPairRDD<String, Integer> numberUserAppRDD = installedAfter2022RDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.cache();

		int maximumInstalls = numberUserAppRDD.values().reduce((x1, x2) -> Math.max(x1, x2));

		JavaRDD<String> output2 = numberUserAppRDD
			.filter(x -> x._2() == maximumInstalls)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
