package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {         // Output value type

    private Double maxPrice;
    private String maxTimestamp; 

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.maxPrice = -1.0;
        this.maxTimestamp = "";
    }

    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        for(Text value : values){
            String[] fields = value.toString().split("_");
            Double price = Double.parseDouble(fields[0]);
            String timestamp = fields[1];

            if(price > this.maxPrice){
                this.maxPrice = price;
                this.maxTimestamp = timestamp;
            }
        }

        context.write(new Text(this.maxPrice.toString()), new Text(this.maxTimestamp));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
