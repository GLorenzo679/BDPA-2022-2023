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
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                IntWritable,           // Output key type
                IntWritable> {  // Output value type  

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer maxYear = 0;
        Integer maxCount = 0;

        for(Text value : values){
            String[] fields = value.toString().split("_");
            Integer year = Integer.parseInt(fields[0]);
            Integer count = Integer.parseInt(fields[1]);

            if(count > maxCount || (count == maxCount && year < maxYear)){
                maxYear = year;
                maxCount = count;
            }
        }

        context.write(new IntWritable(maxYear), new IntWritable(maxCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
