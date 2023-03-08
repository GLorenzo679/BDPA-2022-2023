package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = "sampleData/registerSample.csv";
		inputPath2 = "sampleData/stations.csv";
		threshold = 0.4;
		outputFolder = "data_out";

		// Create a Spark Session object and set the name of the application
		// SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - Template").getOrCreate();

		Dataset<Row> inputRegisterDF = ss.read().format("csv")
										.option("delimiter", "\\t")
										.option("timestampFormat","yyyy-MM-dd HH:mm:ss")
										.option("header", true)
										.option("inferSchema", true)
										.load(inputPath);

		Encoder<registerClass> registerEncoder = Encoders.bean(registerClass.class);

		Dataset<registerClass> registerDS = inputRegisterDF.as(registerEncoder);

		// Filter rows where used_slots and free_slots == 0
		Dataset<registerClass> registerFilteredDS = registerDS.filter(x -> {
			if(x.getFree_Slots() == 0 && x.getUsed_Slots() == 0)
				return false;
			else
				return true;
		});

		// REVIEW
		Dataset<registerNewTimestampClass> timestampRegisterDS = registerFilteredDS.map(x -> {
			registerNewTimestampClass newTimestmap = 
				new registerNewTimestampClass();

				newTimestmap.setStation(x.getStation());
				newTimestmap.setTimestamp(x.getTimestamp());
				newTimestmap.setUsed_Slots(x.getUsed_Slots());
				newTimestmap.setFree_Slots(x.getFree_Slots());
				newTimestmap.setDayofweek(DateTool.DayOfTheWeek(x.getTimestamp()));
				newTimestmap.setHour(DateTool.hour(x.getTimestamp()));

				if(x.getFree_Slots() != 0)
					newTimestmap.setStatus(0);
				else
					newTimestmap.setStatus(1);		

				return newTimestmap;},
			Encoders.bean(registerNewTimestampClass.class));

		RelationalGroupedDataset rgdtimestampRegisterDS = timestampRegisterDS
						.groupBy("station", "dayofweek", "hour");

		Dataset<stationCriticality> criticalityDS = rgdtimestampRegisterDS
							.agg(avg("status"))
							.withColumnRenamed("avg(status)", "criticality")
							.as(Encoders.bean(stationCriticality.class));

		Dataset<stationCriticality> overThresholdDS = criticalityDS.filter("criticality>" + threshold);

		Dataset<Row> inputStationDF = ss.read().format("csv")
										.option("delimiter", "\\t")
										.option("header", true)
										.option("inferSchema", true)
										.load(inputPath2);

		Dataset<station> stationDS = inputStationDF.as(Encoders.bean(station.class));

		Dataset<stationCoordinates> coordinatesDS = overThresholdDS
				.join(stationDS, overThresholdDS.col("station").equalTo(stationDS.col("id")))
				.selectExpr("station", "dayofweek","hour", 
							"longitude", "latitude", "criticality")
				.sort(new Column("criticality").desc(), new Column("station"),
						new Column("dayofweek"), new Column("hour"))
				.as(Encoders.bean(stationCoordinates.class));
		
		coordinatesDS.show();

		// Save the result in the output folder
		coordinatesDS.write().format("csv").option("header", true).save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}
