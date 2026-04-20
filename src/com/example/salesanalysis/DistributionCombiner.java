package com.example.salesanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner for Task 2: Local pre-aggregation of distribution counts.
 *
 * <p>This combiner sums up the per-mapper counts for each
 * (price_range, quantity_bracket) key before sending them to the reducer.
 * Since addition is associative and commutative, this combiner is safe
 * and reduces network I/O significantly.</p>
 *
 * <p><b>Input/Output:</b> Key = "price_range\tquantity_bracket" (Text),
 * Value = partial count (IntWritable)</p>
 *
 * @author BCS017-Midterm
 */
public class DistributionCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    /** Reusable output value. */
    private final IntWritable outValue = new IntWritable();

    /**
     * Sums the local counts for this (price_range, quantity_bracket) pair.
     *
     * @param key     composite key "price_range\tquantity_bracket"
     * @param values  iterator of partial counts
     * @param context MapReduce context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }
}
