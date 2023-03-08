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

		//Username, Gender, YearOfBirth, Country
		inputPath = "data/Users.txt";
		//MID, Title, Director, ReleaseDate
		inputPath2 = "data/Movies.txt";
		//Username, MID, StartTimestamp, EndTimestamp
		inputPath3 = "data/WatchedMovies.txt";

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

		JavaRDD<String> usersInputRDD = sc.textFile(inputPath);
		JavaRDD<String> moviesInputRDD = sc.textFile(inputPath2);
		JavaRDD<String> watchedMoviesInputRDD = sc.textFile(inputPath3);

		//k: MID, v: year
		JavaPairRDD<String, Integer> movieYearLast5YearsRDD = watchedMoviesInputRDD
			.filter(x -> {
				String[] fields = x.split(",");
				String date = fields[2].split("_")[0];

				return date.compareTo("2015/09/17") >= 0
					&& date.compareTo("2020/09/16") <= 0;
			})
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String MID = fields[1];
				String date = fields[2].split("_")[0];

				return new Tuple2<String, Integer>(MID, Integer.parseInt(date.split("/")[0]));
			});

		//k: <MID, year>, v: count time seen
		JavaPairRDD<Tuple2<String, Integer>, Integer> movieYearCountRDD = movieYearLast5YearsRDD
			.mapToPair(x -> new Tuple2<Tuple2<String, Integer>, Integer>(new Tuple2<String, Integer>(x._1(), x._2()), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: MID, v: <year, count time seen>
		//Select only movies watched in one year
		JavaPairRDD<String, Tuple2<Integer, Integer>> moviesOneYear = movieYearCountRDD
			.mapToPair(x -> new Tuple2<String, Tuple2<Integer, Integer>>(x._1()._1(), new Tuple2<Integer, Integer>(x._1()._2(), x._2())))
			.groupByKey()
			.filter(x -> {
				Integer count = 0;

				for(Tuple2<Integer, Integer> e : x._2())
					count++;

				return count == 1;
			})
			.mapValues(x -> x.iterator().next());

		//k: MID, v: year
		//Select only movies seen at leat 1000 times
		JavaPairRDD<String, Integer> output1 = moviesOneYear
			.filter(x -> x._2()._2() >= 1000)
			.mapToPair(x -> new Tuple2<String, Integer>(x._1(), x._2()._1()));

		output1.saveAsTextFile(outputFolder);

		//k: <MID, Year>, v: Username
		//Select distinct pairs k: <MID, Year>, v: Username
		JavaPairRDD<Tuple2<String, String>, String> movieYearRDD = watchedMoviesInputRDD
			.mapToPair(x -> {
				String[] fields = x.split(",");
				String year = fields[2].split("/")[0];

				return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(fields[1], year), fields[0]);
			})
			.distinct();

		//k: <MID, Year>, v: count distinct users
		JavaPairRDD<Tuple2<String, String>, Integer> movieYearCountDistinctUsersRDD = movieYearRDD
			.mapToPair(x -> new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(x._1()._1(), x._1()._2()), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		//k: year, v: MID
		JavaPairRDD<String, String> mostPopularMoviePerYearRDD = movieYearCountDistinctUsersRDD
			.mapToPair(x -> new Tuple2<String, Tuple2<String, Integer>>(x._1()._2(), new Tuple2<String, Integer>(x._1()._1(), x._2())))
			.reduceByKey((x1, x2) -> {
				Tuple2<String, Integer> tmp;

				if(x1._2() > x2._2())
					tmp = x1;
				else
					tmp = x2;
					
				return tmp;
			})
			.mapToPair(x -> new Tuple2<String, String>(x._1(), x._2()._1()));

		//k: MID, v: count years most popular
		JavaPairRDD<String, Integer> yearsMostPopularRDD = mostPopularMoviePerYearRDD
			.mapToPair(x -> new Tuple2<String, Integer>(x._2(), 1))
			.reduceByKey((x1, x2) -> x1 + x2);

		JavaRDD<String> output2 = yearsMostPopularRDD
			.filter(x -> x._2() >= 2)
			.keys();

		output2.saveAsTextFile(outputFolder2);

		// Close the Spark context
		sc.close();
	}
}
