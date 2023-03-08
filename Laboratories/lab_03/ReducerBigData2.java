package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type

    TopKVector<WordCountWritable> top100;

    @Override
    protected void setup(Context context) {
        top100 = new TopKVector<WordCountWritable>(100);
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        int occurrences = 0;

        // Iterate over the set of values and sum them 
        for (IntWritable value : values) {
            occurrences += value.get();
        }

        top100.updateWithNewElement(new WordCountWritable(new String(key.toString()), new Integer(occurrences)));
    }

    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Vector<WordCountWritable> top100Objects = top100.getLocalTopK();

        for (WordCountWritable value : top100Objects) {
            context.write(new Text(value.getWord()), new IntWritable(value.getCount()));
            //System.out.println(value.getWord() + " " + value.getCount());
        }
    }
}
