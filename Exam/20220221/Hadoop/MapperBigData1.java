package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    IntWritable,         // Output key type
                    IntWritable> {// Output value type

    protected void setup(Context context) throws IOException, InterruptedException {

    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        Integer year = Integer.parseInt(fields[0].split("/")[0]);

        context.write(new IntWritable(year), new IntWritable(1));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
