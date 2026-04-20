package com.example.salesanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Task 2: Distribution Analysis by Price Range and Quantity Bracket.
 *
 * <p>Classifies each product into a composite key of
 * "{@code price_range\tquantity_bracket}" and emits a count of 1.</p>
 *
 * <p><b>Price Ranges:</b></p>
 * <ul>
 *   <li>$0-$10: price &lt; 10</li>
 *   <li>$10-$20: 10 &le; price &le; 20</li>
 *   <li>$20+: price &gt; 20</li>
 * </ul>
 *
 * <p><b>Quantity Brackets:</b></p>
 * <ul>
 *   <li>Low Quantity: quantity &lt; 200</li>
 *   <li>Medium Quantity: 200 &le; quantity &le; 400</li>
 *   <li>High Quantity: quantity &gt; 400</li>
 * </ul>
 *
 * <p><b>Output:</b> Key = "price_range\tquantity_bracket" (Text),
 * Value = 1 (IntWritable)</p>
 *
 * @author BCS017-Midterm
 */
public class DistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /** Reusable output key. */
    private final Text outKey = new Text();

    /** Constant ONE for counting. */
    private static final IntWritable ONE = new IntWritable(1);

    /** Counter group name. */
    private static final String COUNTER_GROUP = "SalesAnalysis";

    /**
     * Classifies the price into a display-friendly range label.
     *
     * @param price the unit price
     * @return "$0-$10", "$10-$20", or "$20+"
     */
    private String classifyPriceRange(double price) {
        if (price < 10.0) {
            return "$0-$10";
        } else if (price <= 20.0) {
            return "$10-$20";
        } else {
            return "$20+";
        }
    }

    /**
     * Classifies the quantity into a display-friendly bracket label.
     *
     * @param quantity the total quantity sold
     * @return "Low Quantity", "Medium Quantity", or "High Quantity"
     */
    private String classifyQuantityBracket(int quantity) {
        if (quantity < 200) {
            return "Low Quantity";
        } else if (quantity <= 400) {
            return "Medium Quantity";
        } else {
            return "High Quantity";
        }
    }

    /**
     * Parses each input line, classifies the product by price range
     * and quantity bracket, and emits the composite key with count 1.
     *
     * @param key     byte offset of the line (ignored)
     * @param value   the raw text line
     * @param context MapReduce context
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

        // Validate field count
        if (fields.length != 5) {
            context.getCounter(COUNTER_GROUP, "MalformedLines_Task2").increment(1);
            return;
        }

        try {
            // fields[0] = product_id (not needed)
            // fields[1] = category   (not needed)
            double price   = Double.parseDouble(fields[2].trim());
            int    quantity = Integer.parseInt(fields[3].trim());
            // fields[4] = rating (not needed)

            String priceRange      = classifyPriceRange(price);
            String quantityBracket = classifyQuantityBracket(quantity);

            // Emit composite key: "price_range\tquantity_bracket"
            outKey.set(priceRange + "\t" + quantityBracket);
            context.write(outKey, ONE);

        } catch (NumberFormatException e) {
            // Price or quantity is not a valid number — skip this record
            context.getCounter(COUNTER_GROUP, "ParseErrors_Task2").increment(1);
        }
    }
}
