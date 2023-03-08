package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type

    private String localMaxTimestamp;
    private Double localMaxPrice;

    protected void setup(Context context) throws IOException, InterruptedException {
        this.localMaxTimestamp = "";
        this.localMaxPrice = 0.0;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] fields = value.toString().split(",");
            String stockId = fields[0];
            String date = fields[1];
            String hourMin = fields[2];
            String timestamp = date + "," + hourMin;
            Double price = Double.parseDouble(fields[3]);

            if(date.startsWith("2017") && stockId.equals("GOOG")){
                if(this.localMaxTimestamp == ""){
                    this.localMaxTimestamp = timestamp;
                    this.localMaxPrice = price;
                }
                else if(price > this.localMaxPrice){
                    this.localMaxTimestamp = timestamp;
                    this.localMaxPrice = price;
                }
                else if(price == this.localMaxPrice && timestamp.compareTo(this.localMaxTimestamp) < 0)
                    this.localMaxTimestamp = timestamp;
            }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(this.localMaxPrice + "_" + this.localMaxTimestamp));
    }
}
