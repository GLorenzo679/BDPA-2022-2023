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
                IntWritable,    // Input value type
                IntWritable,           // Output key type
                NullWritable> {  // Output value type

    private Integer numRobots;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.numRobots = 0;
    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        Integer sum = 0;

        for(IntWritable value : values)
            sum++;

        if(sum > 0)
            this.numRobots++;

        /*
            ALTERNATIVE SOLUTION:
            this.numRobots++;
        */
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(this.numRobots), NullWritable.get());

        /*
            ALTERNATIVE SOLUTION:
            context.write(new IntWritable(this.numRobots), NullWritable.get());
        */
    }
}
