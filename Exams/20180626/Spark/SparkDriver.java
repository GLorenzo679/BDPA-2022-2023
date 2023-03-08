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
		String outputFolder;
		String outputFolder2;

		//Timestamp, VSID, CPUUsage%, RAMUsage%
		inputPath = "data/PerformanceLog.txt";

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

		JavaRDD<String> logsInputRDD = sc.textFile(inputPath);

		Double CPUthr = Double.parseDouble(args[1]);
		Double RAMthr = Double.parseDouble(args[2]);

		JavaRDD<String> logsMay2018RDD = logsInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[0];

				return date.startsWith("2018/05");
			})
			.cache();

		// k: VSID_hour, v: <count, <tot cpu, tot ram>>
		JavaPairRDD<String, Tuple2<Integer, Tuple2<Double, Double>>> countRAMCPURDD = logsMay2018RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				Double cpu = Double.parseDouble(fields[3]);
				Double ram = Double.parseDouble(fields[4]);
				Integer hour = Integer.parseInt(fields[1].split(":")[0]);
				String VSID = fields[2];

				return new Tuple2<String, Tuple2<Integer, Tuple2<Double, Double>>>(VSID + "_" + hour, new Tuple2<Integer, Tuple2<Double, Double>>(1, new Tuple2<Double, Double>(cpu, ram)));
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Tuple2<Double, Double>>(x1._1() + x2._1(), new Tuple2<Double, Double>(x1._2()._1() + x2._2()._1(), x1._2()._2() + x2._2()._2())));

		// k: VSID, v: <avg cpu, avg ram>
		JavaPairRDD<String, Tuple2<Double, Double>> avgRAMCPURDD = countRAMCPURDD
			.mapToPair(x -> new Tuple2<String, Tuple2<Double, Double>>(x._1(), new Tuple2<Double, Double>(new Double(x._2()._2()._1()/x._2()._1()), new Double(x._2()._2()._2()/x._2()._1()))));
		
		JavaRDD<String> output1 = avgRAMCPURDD
			.filter(x -> x._2()._1() > CPUthr && x._2()._2() > RAMthr)
			.keys();

		output1.saveAsTextFile(outputFolder);

		// k: <VSID_date, hour>, v: cpu usage
		JavaPairRDD<Tuple2<String, Integer>, Double> cpuDateRDD = logsMay2018RDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String vsid = fields[2];
				String date = fields[0];
				Integer hour = Integer.parseInt(fields[1].split(":")[0]);
				Double cpu = Double.parseDouble(fields[3]);

				return new Tuple2<Tuple2<String, Integer>, Double>(new Tuple2<String, Integer>(vsid + date, hour), cpu);
			});

		// select max cpu usage per vsid date hour tuple
		// select only the lines with maxCPUUtilization > 90 or < 10
		JavaPairRDD<Tuple2<String, Integer>, Double> maxCpuDateRDD = cpuDateRDD
			.reduceByKey((x1, x2) -> {
				Double max = Math.max(x1, x2);

				return max;
			})
			.filter(x -> x._2() < 10 || x._2() > 90);

		// k: VSID_date, v: <count < 10, count > 90>
		JavaPairRDD<String, Tuple2<Integer, Integer>> over8HoursRDD = maxCpuDateRDD
			.mapToPair(x -> {

				if(x._2() > 90)
					return new Tuple2<String, Tuple2<Integer, Integer>>(x._1()._1(), new Tuple2<Integer, Integer>(0, 1));
				else
					return new Tuple2<String, Tuple2<Integer, Integer>>(x._1()._1(), new Tuple2<Integer, Integer>(1, 0));
			})
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._2(), x1._2() + x2._2()));

		JavaRDD<String> output2 = over8HoursRDD
			.filter(x -> x._2()._1() >= 8 && x._2()._2() >= 8)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
