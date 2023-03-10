package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    Text,         // Output key type
                    CustomClassWritable> {// Output value type

    protected void setup(Context context) throws IOException, InterruptedException {
        //filter out header line
        context.nextKeyValue();
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String fields [] = value.toString().split(",");
            
            CustomClassWritable pair = new CustomClassWritable(fields[1], Float.valueOf(fields[6]));

            context.write(new Text(fields[2]), pair);
    }
}
