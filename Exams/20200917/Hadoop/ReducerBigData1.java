package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        boolean oneUsername = true;
        String firstUsername = null;

		/* Implement the reduce method */

        // Iterate over the set of values and check if there are at least two usernames
		for (Text value : values) {
			String username = value.toString();

			if (firstUsername == null)
				firstUsername = username;
			else
				if (firstUsername.compareTo(username)!=0)
					oneUsername = false;
		}

		if (oneUsername == true) 
			context.write(new Text(key), NullWritable.get());     
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
