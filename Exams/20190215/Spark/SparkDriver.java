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

		//POI_ID, latitude, longitude, city, country, category, subcategory
		inputPath = "data/POIs.txt";

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

		JavaRDD<String> poiInputRDD = sc.textFile(inputPath);

		JavaRDD<String> italianCitiesRDD = poiInputRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[4].equals("Italy");
			})
			.cache();

		//k: city, v: <0/1, 0/1> taxi or bus
		JavaPairRDD<String, Tuple2<Integer, Integer>> cityTaxiBusOnesRDD = italianCitiesRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return (fields[6].equals("Taxi") || fields[6].equals("Busstop"));
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String city = fields[3];
				String subCategory = fields[6];

				if(subCategory.equals("Taxi"))
					return new Tuple2<String, Tuple2<Integer, Integer>>(city, new Tuple2<Integer, Integer>(1, 0));
				else
					return new Tuple2<String, Tuple2<Integer, Integer>>(city, new Tuple2<Integer, Integer>(0, 1));
			});

		//k: city, v: <count_taxi, count_bus>
		//Select only cities where taxi count > 0 and bus count == 0
		JavaPairRDD<String, Tuple2<Integer, Integer>> cityTaxiBusCountRDD = cityTaxiBusOnesRDD
			.reduceByKey((x1, x2) -> new Tuple2<Integer, Integer>(x1._1() + x2._1(), x1._2() + x2._2()))
			.filter(x -> (x._2()._1() > 0 && x._2()._2() == 0));
		
		JavaRDD<String> output1 = cityTaxiBusCountRDD
			.keys();

		output1.saveAsTextFile(outputFolder);

		//Select only POI with museum
		JavaRDD<String> cityMuseumRDD = italianCitiesRDD
			.filter(x -> {
				String[] fields = x.split(",");

				return fields[6].equals("museum");
			});

		//k: city, v: museum count
		JavaPairRDD<String, Integer> cityMuseumCountRDD = cityMuseumRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");

				return new Tuple2<String, Integer>(fields[3], 1);
			})
			.reduceByKey((x1, x2) -> x1 + x2);

		//distinct cities in italy
		JavaRDD<String> cityRDD = italianCitiesRDD
			.map(x -> {
				String[] fields = x.split(",");

				return fields[3];
			})
			.distinct();

		Long numCities = cityRDD.count();

		Integer numMuseum = cityMuseumCountRDD
			.values()
			.reduce((x1, x2) -> x1 + x2);

		Double avg = numMuseum/(new Double(numCities));

		JavaRDD<String> output2 = cityMuseumCountRDD
			.filter(x -> x._2() > avg)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
