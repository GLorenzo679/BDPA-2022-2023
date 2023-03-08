package it.polito.bigdata.spark.example;

import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

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
		Double threshold;
		String outputFolder;

		inputPath = "data/sampleData/registerSample.csv";
		inputPath2 = "data/sampleData/stations.csv";
		threshold = 0.4;
		//threshold = Double.parseDouble(args[0]);
		outputFolder = "data_out";

		// Create a configuration object and set the name of the application
		//SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Task 1

		// Read CSV input info
		JavaRDD<String> registerInputRDD = sc.textFile(inputPath);  
		JavaRDD<String> stationsInputRDD = sc.textFile(inputPath2); 

		// Remove header
		registerInputRDD = registerInputRDD.filter(x -> !x.contains("station")); 

		// Process register data
		JavaRDD<registerClass> registerRDD = registerInputRDD.map(x -> {
			String[] fields = x.split("\\t");
			String[] date_hour = fields[1].split(" ");

			return new registerClass(Integer.valueOf(fields[0]), date_hour[0], date_hour[1], Integer.valueOf(fields[2]), Integer.valueOf(fields[3]));
		});

		// Filter wrong data -> used_slots == 0 && free_slots == 0
		JavaRDD<registerClass> registerFilteredRDD = registerRDD.filter(x -> {
			return !(x.getUsedSlots() == 0 && x.getFreeSlots() == 0);
		});

		// Create pair RDD in the format K: timeslot, V: registerClass
		JavaPairRDD<String, Iterable<registerClass>> weekDayRegisterRDD = registerFilteredRDD.mapToPair(x -> {
			return new Tuple2<String, registerClass>(x.getStation() + "_" + x.getTimeslot(), x);
		}).groupByKey();

		JavaPairRDD<String, Double> criticalityRDD = weekDayRegisterRDD.mapToPair(x -> {
			List<registerClass> registerList = new ArrayList<>();
			x._2().iterator().forEachRemaining(registerList::add);
			Double noFreeslots = 0.0;

			for(registerClass register : registerList)
				if(register.getFreeSlots() == 0)
					noFreeslots++;

			return new Tuple2<String, Double>(x._1(), noFreeslots/registerList.size());
		});

		JavaPairRDD<String, Double> overThresholdRDD = criticalityRDD.filter(x -> x._2() >= threshold);

		JavaPairRDD<String, Tuple2<String, Double>> stationTimeslotCritRDD = overThresholdRDD.mapToPair(x -> {
			String[] fields = x._1().split("_");
			String station = fields[0];
			String timestamp = fields[1] + "_" + fields[2];
			return new Tuple2<String, Tuple2<String, Double>>(station, new Tuple2<String, Double>(timestamp, x._2()));
		});

		JavaPairRDD<String, Tuple2<String, Double>> resultRDD = stationTimeslotCritRDD.reduceByKey((x1, x2) -> {
			if(x1._2() > x2._2())
				return new Tuple2<String,Double>(x1._1(), x1._2());
			else if(x1._2() == x2._2()){
				String[] fields1 = x1._1().split("_");
				String[] fields2 = x2._1().split("_");

				if(fields1[1].compareTo(fields2[1]) < 0)
					return new Tuple2<String,Double>(x1._1(), x1._2());
				else if(fields1[1].compareTo(fields2[1]) > 0)
					return new Tuple2<String,Double>(x2._1(), x2._2());
				else{
					if(fields1[0].compareTo(fields2[0]) < 0)
						return new Tuple2<String,Double>(x1._1(), x1._2());
					else
						return new Tuple2<String,Double>(x2._1(), x2._2());
				}
			}
			else
				return new Tuple2<String,Double>(x2._1(), x2._2());
		});

		resultRDD.saveAsTextFile(outputFolder);

		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		// resultKML.coalesce(1).saveAsTextFile(outputFolder); 

		// Close the Spark context
		sc.close();
	}
}
