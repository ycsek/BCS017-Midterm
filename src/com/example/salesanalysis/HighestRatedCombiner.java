package com.example.salesanalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner for Task 1: Local pre-aggregation of highest-rated products.
 *
 * <p>This combiner runs on the mapper node and reduces network traffic by
 * filtering out products that are clearly not the highest-rated within
 * each category on that particular mapper's split. Only products matching
 * the local maximum rating are forwarded to the reducer.</p>
 *
 * <p><b>Input/Output:</b> Key = category (Text), Value = "product_id\trating" (Text)</p>
 *
 * @author BCS017-Midterm
 */
public class HighestRatedCombiner extends Reducer<Text, Text, Text, Text> {

    /** Reusable output value. */
    private final Text outValue = new Text();

    /**
     * Finds the local maximum rating for this category and emits only
     * products that match that maximum. This is safe because the global
     * maximum must be at least as large as any local maximum, so we
     * only discard products that cannot be the global winner.
     *
     * @param key     the category name
     * @param values  iterator of "product_id\trating" strings
     * @param context MapReduce context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int maxRating = Integer.MIN_VALUE;
        // We need two passes: first find max, then emit matches.
        // Since Iterable can only be traversed once, we buffer the values.
        java.util.List<String> entries = new java.util.ArrayList<>();

        for (Text val : values) {
            String entry = val.toString();
            entries.add(entry);

            String[] parts = entry.split("\t");
            if (parts.length == 2) {
                int rating = Integer.parseInt(parts[1]);
                if (rating > maxRating) {
                    maxRating = rating;
                }
            }
        }

        // Emit only entries that match the local maximum
        for (String entry : entries) {
            String[] parts = entry.split("\t");
            if (parts.length == 2) {
                int rating = Integer.parseInt(parts[1]);
                if (rating == maxRating) {
                    outValue.set(entry);
                    context.write(key, outValue);
                }
            }
        }
    }
}
