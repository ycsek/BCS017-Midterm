package com.example.salesanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Task 1: Most Highly-Rated Product per Category.
 *
 * <p>Receives all "product_id\trating" values for a single category,
 * finds the global maximum rating, and emits every product that
 * shares that maximum rating.</p>
 *
 * <p><b>Output format:</b> ASCII table with columns:
 * {@code | Category | Most Popular Product | Highest Rating |}</p>
 *
 * @author BCS017-Midterm
 */
public class HighestRatedReducer extends Reducer<Text, Text, Text, NullWritable> {

    /** Reusable output key. */
    private final Text outKey = new Text();

    /** Whether the table header has been printed yet. */
    private boolean headerPrinted = false;

    /**
     * Prints the ASCII table header on the first call.
     *
     * @param context MapReduce context
     */
    private void printHeaderIfNeeded(Context context)
            throws IOException, InterruptedException {
        if (!headerPrinted) {
            outKey.set(String.format("| %-15s | %-22s | %-15s |",
                    "Category", "Most Popular Product", "Highest Rating"));
            context.write(outKey, NullWritable.get());

            outKey.set(String.format("| %-15s | %-22s | %-15s |",
                    "---------------", "----------------------", "---------------"));
            context.write(outKey, NullWritable.get());

            headerPrinted = true;
        }
    }

    /**
     * Iterates over all products in the category to find the maximum
     * rating, then emits all products achieving that rating in table format.
     *
     * @param key     the category name
     * @param values  iterator of "product_id\trating" strings
     * @param context MapReduce context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Print table header before the first data row
        printHeaderIfNeeded(context);

        String category = key.toString();
        int maxRating = Integer.MIN_VALUE;

        // Buffer all entries since we need two passes
        List<String[]> productRatingPairs = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length == 2) {
                String productId = parts[0];
                int rating = Integer.parseInt(parts[1]);

                productRatingPairs.add(new String[]{productId, String.valueOf(rating)});

                if (rating > maxRating) {
                    maxRating = rating;
                }
            }
        }

        // Emit all products that match the global maximum rating
        for (String[] pair : productRatingPairs) {
            int rating = Integer.parseInt(pair[1]);
            if (rating == maxRating) {
                // Output format: | Category | Most Popular Product | Highest Rating |
                outKey.set(String.format("| %-15s | %-22s | %-15d |",
                        category, pair[0], rating));
                context.write(outKey, NullWritable.get());
            }
        }
    }
}
