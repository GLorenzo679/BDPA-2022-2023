package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer hardDriveFault = 0;
        Integer RAMFault = 0;

        for(Text value : values){
            if(value.toString().equals("RAM"))
                RAMFault = 1;
            else
                hardDriveFault = 1;
        }

        if(RAMFault == 1 && hardDriveFault == 1)
            context.write(key, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
