package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

    		/* Implement the map method */ 
            String[] fields = value.toString().split(",");
            String SID = fields[2];
            String date = fields[0];
            String FailureType = fields[3];

            if(date.startsWith("2016/04") && (FailureType.equals("RAM") || FailureType.equals("hard_drive")))
                context.write(new Text(SID), new Text(FailureType));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
