package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            IntWritable key,   // Input key type
            IntWritable value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            context.write(NullWritable.get(), new Text(key.toString() + "_" + value.toString()));
    }
}
