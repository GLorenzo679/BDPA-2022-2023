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
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
            String fields[] = value.toString().split("\\s+");
            String products[] = fields[0].split(",");
            String temp;

            if(products[0].compareTo(products[1]) > 0){
                String swap = products[1];
                products[1] = products[0];
                products[0] = swap;
                temp = products[0] + "," + products[1];
            }
            else
                temp = products[0] + "," + products[1];
                
            context.write(new Text(temp), new IntWritable(Integer.valueOf(Integer.parseInt(fields[1]))));
    }
}
