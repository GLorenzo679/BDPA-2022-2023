package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {         // Output value type

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer numManufacturer = 0;
        String manufacturer = "";

        for(Text value : values){
            if(manufacturer.equals("")){
                manufacturer = value.toString();
                numManufacturer++;
            }
            else if(!manufacturer.equals(value.toString()))
                numManufacturer++;        
        }

        if(numManufacturer == 1)
            context.write(new Text(key.toString() + "," + manufacturer), NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
