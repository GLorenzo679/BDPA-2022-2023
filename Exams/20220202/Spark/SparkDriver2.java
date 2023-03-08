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

		//AppId, AppName, Price, Category, Company
		inputPath = "data/Apps.txt";
		//UserId, AppId, Timestamp, Action
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

		JavaRDD<String> appInputRDD = sc.textFile(inputPath);
		JavaRDD<String> actionsInputRDD = sc.textFile(inputPath2);

		// k: appId, v: AppName
		JavaPairRDD<String, String> appIdNameRDD = appInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, String>(fields[0], fields[1]);
			});

		// select only 2021 apps with install or remove actions
		JavaRDD<String> app2021RDD = actionsInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[2];
				String action = fields[3];
				
				return date.startsWith("2021") && (action.equals("Install") || action.equals("Remove"));
			})
			.cache();

		// k: <appId, month>, v: <count install, count remove>
		JavaPairRDD<Tuple2<String, Integer>, Tuple2<Integer, Integer>> app2021CountRDD = app2021RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String appId = fields[1];
				String action = fields[3];
				Integer month = Integer.parseInt(fields[2].split("/")[1]);

				if(action.equals("Install"))
					return new Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>
						(new Tuple2<String, Integer>(appId, month), new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Integer>>
						(new Tuple2<String, Integer>(appId, month), new Tuple2<Integer, Integer>(0, 1));
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()));

		// k: appID, v: count month
		JavaPairRDD<String, Integer> moreInstall2021AppRDD = app2021CountRDD
			.filter(x -> x._2()._1() > x._2()._2())
			.mapToPair(x -> new Tuple2<String, Integer>(x._1()._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		JavaPairRDD<String, Integer> goodApps = moreInstall2021AppRDD
			.filter(x -> x._2() == 12);

		JavaPairRDD<String, String> output1 = appIdNameRDD
			.join(goodApps)
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()._1()));

		output1.saveAsTextFile(outputFolder);

		// select only 2022 installed apps
		JavaRDD<String> app2022RDD = actionsInputRDD	
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[2].startsWith("2022") && fields[3].equals("Install");
			});

		// select only 2021 installed apps
		JavaRDD<String> app2021InstallRDD = app2021RDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[3].equals("Install");
			});

		// k: appId, v: userId
		JavaPairRDD<String, String> appUser2022OnesRDD = app2022RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String user = fields[0];
				String app = fields[1];

				return new Tuple2<String, String>(app, user);
			})
			.distinct();

		// k: appId, v: userId
		JavaPairRDD<String, String> appUser2021OnesRDD = app2021InstallRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String user = fields[0];
				String app = fields[1];

				return new Tuple2<String, String>(app, user);
			})
			.distinct();

		// k: appId, v: count users
		JavaPairRDD<String,  Integer> appUser2022CountRDD = appUser2022OnesRDD
			.subtract(appUser2021OnesRDD)
			.mapToPair(x -> new Tuple2<String, Integer>(x._1(), 1))
			.reduceByKey((x1, x2) -> x1 + x2)
			.cache();

		Integer maxUsers = appUser2022CountRDD
			.values()
			.reduce((x1, x2) -> {
				Integer max = Math.max(x1, x2);

				return max;
			});

		JavaRDD<String> output2 = appUser2022CountRDD
			.filter(x -> x._2() == maxUsers)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
