package com.example.salesanalysis;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Task 2: Distribution Analysis by Price Range and Quantity Bracket.
 *
 * <p>Collects all (price_range, quantity_bracket) counts and outputs a
 * <b>pivot table</b> with price ranges as rows and quantity brackets
 * as columns.</p>
 *
 * <p><b>Output format (ASCII table):</b></p>
 * <pre>
 * | Price Range | Low Quantity | Medium Quantity | High Quantity |
 * | ----------- | ------------ | --------------- | ------------- |
 * | $0-$10      | 196          | 239             | 777           |
 * | $10-$20     | 193          | 259             | 779           |
 * | $20+        | 1473         | 2025            | 6059          |
 * </pre>
 *
 * @author BCS017-Midterm
 */
public class DistributionReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    /** Reusable output key. */
    private final Text outKey = new Text();

    /** Ordered price ranges for row ordering. */
    private static final String[] PRICE_RANGES = {"$0-$10", "$10-$20", "$20+"};

    /** Ordered quantity brackets for column ordering. */
    private static final String[] QTY_BRACKETS = {"Low Quantity", "Medium Quantity", "High Quantity"};

    /**
     * Accumulator: stores counts for each (price_range, quantity_bracket) cell.
     * Populated during reduce() calls, flushed as a pivot table in cleanup().
     */
    private final Map<String, Map<String, Integer>> matrix = new LinkedHashMap<>();

    /**
     * Initializes the pivot table matrix with zeroes.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (String pr : PRICE_RANGES) {
            Map<String, Integer> row = new LinkedHashMap<>();
            for (String qb : QTY_BRACKETS) {
                row.put(qb, 0);
            }
            matrix.put(pr, row);
        }
    }

    /**
     * Accumulates counts for each (price_range, quantity_bracket) pair.
     * The actual table output is deferred to {@link #cleanup(Context)}.
     *
     * @param key     composite key "price_range\tquantity_bracket"
     * @param values  iterator of counts (each = 1, or pre-combined sums)
     * @param context MapReduce context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalCount = 0;
        for (IntWritable val : values) {
            totalCount += val.get();
        }

        // Parse the composite key
        String[] parts = key.toString().split("\t");
        String priceRange      = parts[0];
        String quantityBracket = parts[1];

        // Store in the pivot matrix
        if (matrix.containsKey(priceRange) && matrix.get(priceRange).containsKey(quantityBracket)) {
            matrix.get(priceRange).put(quantityBracket, totalCount);
        }
    }

    /**
     * Outputs the complete pivot table after all reduce() calls finish.
     * This guarantees the header appears once and all rows are properly aligned.
     *
     * @param context MapReduce context
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Print header row
        outKey.set(String.format("| %-12s | %-14s | %-17s | %-14s |",
                "Price Range", "Low Quantity", "Medium Quantity", "High Quantity"));
        context.write(outKey, NullWritable.get());

        // Print separator row
        outKey.set(String.format("| %-12s | %-14s | %-17s | %-14s |",
                "------------", "--------------", "-----------------", "--------------"));
        context.write(outKey, NullWritable.get());

        // Print data rows in the defined order
        for (String pr : PRICE_RANGES) {
            Map<String, Integer> row = matrix.get(pr);
            outKey.set(String.format("| %-12s | %-14d | %-17d | %-14d |",
                    pr,
                    row.get("Low Quantity"),
                    row.get("Medium Quantity"),
                    row.get("High Quantity")));
            context.write(outKey, NullWritable.get());
        }
    }
}
