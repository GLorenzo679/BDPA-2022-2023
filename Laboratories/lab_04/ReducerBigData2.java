package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        ArrayList<FloatWritable> tmpList = new ArrayList<>();
        Float tempSum = 0f;
        Float avg = 0f;

        for (FloatWritable value:values){
            tmpList.add(value);
            tempSum += value.get();
        }

        avg = (float)tempSum/tmpList.size();

        context.write(key, new FloatWritable(avg));
    }
}
