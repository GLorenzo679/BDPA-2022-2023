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
                IntWritable,           // Input key type
                IntWritable,    // Input value type
                IntWritable,           // Output key type
                IntWritable> {         // Output value type

    private Integer maxYear;
    private Integer maxCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.maxYear = 0;
        this.maxCount = 0;
    }

    @Override
    protected void reduce(
        IntWritable key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        Integer sum = 0;
        Integer year = key.get();

        for(IntWritable value : values)
            sum += value.get();
        
        if(sum > this.maxCount || (sum == this.maxCount && year < this.maxYear)){
            this.maxCount = sum;
            this.maxYear = year;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(this.maxYear), new IntWritable(this.maxCount));
    }
}
