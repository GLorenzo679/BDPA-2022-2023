package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type  

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        String maxOs = "";
        Integer maxCount = 0;

        for(Text value : values){
            String[] fields = value.toString().split("_"); 
            String os = fields[0];
            Integer count = Integer.parseInt(fields[1]);

            if(maxOs == "" || count > maxCount || (count == maxCount && os.compareTo(maxOs) < 0)){
                maxOs = os;
                maxCount = count;
            }
        }

        context.write(new Text(maxOs), NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
