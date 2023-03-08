package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                Text> {         // Output value type

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer sum = 0;

		/* Implement the reduce method */
        for(IntWritable value : values)
            sum = sum + value.get();

        if(sum >= 2){
            String CustomerID = key.toString().split("_")[0];
            String BID = key.toString().split("_")[1];

            context.write(new Text(CustomerID), new Text(BID));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
