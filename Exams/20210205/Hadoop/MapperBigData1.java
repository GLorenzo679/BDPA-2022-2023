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
                    Text,         // Output key type
                    IntWritable> {// Output value type

    protected void setup(Context context) throws IOException, InterruptedException {

    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] fields = value.toString().split(",");
            String RID = fields[0];
            String FailureTypeCode = fields[1];

            if(FailureTypeCode.equals("FCode122"))
                context.write(new Text(RID), new IntWritable(1));

            /*
                ALTERNATIVE SOLUTION:
                if(FailureTypeCode.equals("FCode122"))
                                context.write(new Text(RID), NullWritable.get());
            */
    }
}
