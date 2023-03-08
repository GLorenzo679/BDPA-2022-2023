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
                
                
            String[] fields = value.toString().split(",");
            String timestamp = fields[0];
            Integer year = Integer.parseInt(timestamp.split("/")[0]);
            Integer month = Integer.parseInt(timestamp.split("/")[1]);
            Integer hour = Integer.parseInt(fields[1].split(":")[0]);
            String VSID = fields[2];
            Double cpuUsage = Double.parseDouble(fields[3]);
            
            if(year == 2018 && 
                month == 5 &&
                hour >= 9 &&
                hour <= 17 && 
                cpuUsage > 99.8){

                context.write(new Text(VSID), new IntWritable(1));
            }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
