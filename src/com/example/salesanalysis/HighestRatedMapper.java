package com.example.salesanalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Mapper for Task 1: Most Highly-Rated Product per Category.
 *
 * <p><b>Input:</b> Each line from {@code sales_data.txt} in the format:
 * {@code product_id,category,price,quantity,rating}</p>
 *
 * <p><b>Output:</b> Key = category (Text), Value = "product_id\trating" (Text).
 * We emit the category as the key so the reducer receives all products
 * in the same category together, and can determine the maximum rating.</p>
 *
 * <p>Malformed lines are counted via a Hadoop counter and skipped.</p>
 *
 * @author BCS017-Midterm
 */
public class HighestRatedMapper extends Mapper<LongWritable, Text, Text, Text> {

    /** Reusable output key to avoid object creation overhead. */
    private final Text outKey = new Text();

    /** Reusable output value to avoid object creation overhead. */
    private final Text outValue = new Text();

    /**
     * Counter group name for tracking malformed/skipped records.
     */
    private static final String COUNTER_GROUP = "SalesAnalysis";

    /**
     * Parses each input line, extracts the category as key and
     * "product_id\trating" as value.
     *
     * @param key     byte offset of the line (ignored)
     * @param value   the raw text line
     * @param context MapReduce context for emitting key-value pairs
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines
        if (line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");

        // Validate field count: product_id, category, price, quantity, rating
        if (fields.length != 5) {
            context.getCounter(COUNTER_GROUP, "MalformedLines_Task1").increment(1);
            return;
        }

        try {
            String productId = fields[0].trim();
            String category  = fields[1].trim();
            // fields[2] = price  (not needed for this task)
            // fields[3] = quantity (not needed for this task)
            int rating = Integer.parseInt(fields[4].trim());

            // Emit: key=category, value="product_id\trating"
            outKey.set(category);
            outValue.set(productId + "\t" + rating);
            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Rating is not a valid integer — skip this record
            context.getCounter(COUNTER_GROUP, "ParseErrors_Task1").increment(1);
        }
    }
}
