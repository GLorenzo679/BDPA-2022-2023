package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,                   // Input key type
                Text,                   // Input value type
                Text,                   // Output key type
                IntWritable> {    // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        List<String> products = new ArrayList<>();

        for (Text value : values)
            products.add(value.toString());

        for (int i = 0; i < products.size() - 1; i++) {
            for (int j = i + 1; j < products.size(); j++) {
                if(products.get(i).compareTo(products.get(j)) != 0) {
                    String  product_pair = products.get(i) + "," + products.get(j);
                    context.write(new Text(product_pair), new IntWritable(1));
                }
            }
        }
    }
}
