package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                CustomClassWritable,    // Input value type
                CustomClassWritable,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<CustomClassWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

        ArrayList<CustomClassWritable> tmpList = new ArrayList<>();
        Float tempSum = 0f;
        Float avg = 0f;

        for (CustomClassWritable value:values){
            tmpList.add(new CustomClassWritable(value.getPid(), value.getScore()));
            tempSum += value.getScore();
        }

        avg = (float)tempSum/tmpList.size();
        
        for (CustomClassWritable value:tmpList){
            value.setScore(value.getScore()-avg);
            context.write(value, NullWritable.get());
        }
    }
}
