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
                Text,           // Output key type
                IntWritable> {         // Output value type

    private String maxOs;
    private Integer maxCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.maxOs = "";
        this.maxCount = 0;
    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String os = key.toString();
        Integer count = 0;

        for(IntWritable value : values)
            count += value.get();

        if(count > this.maxCount){
            this.maxOs = os;
            this.maxCount = count;
        }
        else if(count == this.maxCount && os.compareTo(this.maxOs) < 0)
            this.maxOs = os;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //compute local maximum
        context.write(new Text(maxOs), new IntWritable(maxCount));
    }
}
