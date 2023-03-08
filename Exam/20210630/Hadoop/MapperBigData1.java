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
                    Text> {// Output value type

    protected void setup(Context context) throws IOException, InterruptedException {

    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        String date = fields[3];
        String eu = fields[6];
        String modelId = fields[2];

        if(date.startsWith("2020"))
            context.write(new Text(modelId), new Text(eu));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
