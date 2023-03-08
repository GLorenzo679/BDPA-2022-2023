package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String lines[] = value.toString().toLowerCase().split("\n");

            for (String line:lines) {
                if (line.startsWith(context.getConfiguration().get("startString"))) {
                    String fields[] = line.split("\t");
                    context.getCounter(COUNTERS.SELECTED_WORDS).increment(1);
                    context.write(new Text(fields[0]), new IntWritable(Integer.parseInt(fields[1])));
                }
                else
                    context.getCounter(COUNTERS.DISCARDED_WORDS).increment(1);
            }
    }
}
