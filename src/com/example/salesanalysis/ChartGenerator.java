package com.example.salesanalysis;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.GradientPaint;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.chart.title.TextTitle;
import org.jfree.chart.ui.HorizontalAlignment;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 * Post-processing class that reads MapReduce output files and generates
 * publication-quality bar chart visualizations using JFreeChart.
 *
 * <p>Generates two PNG charts:</p>
 * <ol>
 *   <li><b>highest_rated_per_category.png</b> — Bar chart showing the highest
 *       rating achieved in each product category.</li>
 *   <li><b>distribution_by_price_quantity.png</b> — Grouped bar chart showing
 *       the count of products in each (price_range, quantity_bracket) cell.</li>
 * </ol>
 *
 * <h3>Usage:</h3>
 * <pre>
 *   java -cp sales-analysis.jar:lib/* \
 *       com.example.salesanalysis.ChartGenerator \
 *       &lt;task1_output_dir&gt; &lt;task2_output_dir&gt; &lt;chart_output_dir&gt;
 * </pre>
 *
 * @author BCS017-Midterm
 */
public class ChartGenerator {

    // ====================================================================
    // Color palette — modern, vibrant, and visually distinct
    // ====================================================================

    /** Colors for the category bar chart (one per category). */
    private static final Color[] CATEGORY_COLORS = {
        new Color(99, 102, 241),   // Indigo
        new Color(236, 72, 153),   // Pink
        new Color(34, 197, 94),    // Green
        new Color(251, 146, 60),   // Orange
        new Color(14, 165, 233),   // Sky blue
        new Color(168, 85, 247),   // Purple
        new Color(245, 158, 11),   // Amber
        new Color(239, 68, 68),    // Red
    };

    /** Colors for the grouped bar chart (one per quantity bracket). */
    private static final Color[] QUANTITY_COLORS = {
        new Color(59, 130, 246),   // Blue
        new Color(16, 185, 129),   // Emerald
        new Color(249, 115, 22),   // Orange
    };

    /** Chart background color (dark theme). */
    private static final Color BG_COLOR = new Color(30, 30, 46);

    /** Plot background color. */
    private static final Color PLOT_BG = new Color(40, 40, 60);

    /** Grid line color. */
    private static final Color GRID_COLOR = new Color(70, 70, 100);

    /** Text color for labels and titles. */
    private static final Color TEXT_COLOR = new Color(230, 230, 250);

    // ====================================================================
    // Fonts
    // ====================================================================

    private static final Font TITLE_FONT    = new Font("SansSerif", Font.BOLD, 22);
    private static final Font SUBTITLE_FONT = new Font("SansSerif", Font.PLAIN, 14);
    private static final Font AXIS_FONT     = new Font("SansSerif", Font.BOLD, 14);
    private static final Font TICK_FONT     = new Font("SansSerif", Font.PLAIN, 12);
    private static final Font LEGEND_FONT   = new Font("SansSerif", Font.PLAIN, 13);

    /**
     * Reads Task 1 output and returns a map of category -> max rating.
     * Since multiple products may share the max rating, we only keep
     * the rating value (for the bar chart).
     *
     * @param task1Dir directory containing part-r-00000 output
     * @return ordered map of category -> highest rating
     */
    private static Map<String, Integer> readTask1Output(String task1Dir) throws IOException {
        Map<String, Integer> categoryRatings = new TreeMap<>();
        File outputFile = new File(task1Dir, "part-r-00000");

        if (!outputFile.exists()) {
            throw new IOException("Task 1 output file not found: " + outputFile.getAbsolutePath());
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Skip header and separator lines
                if (line.contains("Category") || line.contains("---")) continue;

                // Format: | Category        | Most Popular Product   | Highest Rating  |
                String[] parts = line.split("\\|");
                if (parts.length >= 4) {
                    String category = parts[1].trim();
                    int rating = Integer.parseInt(parts[3].trim());
                    // Keep the max (they should all be the same for a given category)
                    categoryRatings.merge(category, rating, Math::max);
                }
            }
        }

        return categoryRatings;
    }

    /**
     * Reads Task 2 output and returns a map of
     * (price_range, quantity_bracket) -> count.
     *
     * @param task2Dir directory containing part-r-00000 output
     * @return ordered map preserving insertion order
     */
    private static Map<String, Map<String, Integer>> readTask2Output(String task2Dir)
            throws IOException {

        // Use LinkedHashMap to maintain a custom order for price ranges
        Map<String, Map<String, Integer>> distribution = new LinkedHashMap<>();

        // Pre-initialize the structure with desired order
        String[] priceRanges = {"$0-$10", "$10-$20", "$20+"};
        String[] quantityBrackets = {"Low Quantity", "Medium Quantity", "High Quantity"};
        for (String pr : priceRanges) {
            Map<String, Integer> inner = new LinkedHashMap<>();
            for (String qb : quantityBrackets) {
                inner.put(qb, 0);
            }
            distribution.put(pr, inner);
        }

        File outputFile = new File(task2Dir, "part-r-00000");
        if (!outputFile.exists()) {
            throw new IOException("Task 2 output file not found: " + outputFile.getAbsolutePath());
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Skip header and separator lines
                if (line.contains("Price Range") || line.contains("---")) continue;

                // Pivot table format: | PriceRange | LowQty | MedQty | HighQty |
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String priceRange = parts[1].trim();
                    int lowQty    = Integer.parseInt(parts[2].trim());
                    int medQty    = Integer.parseInt(parts[3].trim());
                    int highQty   = Integer.parseInt(parts[4].trim());

                    if (distribution.containsKey(priceRange)) {
                        distribution.get(priceRange).put("Low Quantity", lowQty);
                        distribution.get(priceRange).put("Medium Quantity", medQty);
                        distribution.get(priceRange).put("High Quantity", highQty);
                    }
                }
            }
        }

        return distribution;
    }

    /**
     * Applies the dark theme styling to a category plot.
     *
     * @param chart the JFreeChart to style
     */
    private static void applyDarkTheme(JFreeChart chart) {
        chart.setBackgroundPaint(BG_COLOR);
        chart.getTitle().setPaint(TEXT_COLOR);
        chart.getTitle().setFont(TITLE_FONT);

        if (chart.getLegend() != null) {
            chart.getLegend().setBackgroundPaint(BG_COLOR);
            chart.getLegend().setItemPaint(TEXT_COLOR);
            chart.getLegend().setItemFont(LEGEND_FONT);
        }

        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(PLOT_BG);
        plot.setRangeGridlinePaint(GRID_COLOR);
        plot.setDomainGridlinePaint(GRID_COLOR);
        plot.setOutlinePaint(GRID_COLOR);
        plot.setRangeGridlineStroke(new BasicStroke(0.5f));

        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setTickLabelPaint(TEXT_COLOR);
        domainAxis.setTickLabelFont(TICK_FONT);
        domainAxis.setLabelPaint(TEXT_COLOR);
        domainAxis.setLabelFont(AXIS_FONT);

        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setTickLabelPaint(TEXT_COLOR);
        rangeAxis.setTickLabelFont(TICK_FONT);
        rangeAxis.setLabelPaint(TEXT_COLOR);
        rangeAxis.setLabelFont(AXIS_FONT);
    }

    /**
     * Generates the Task 1 bar chart: Highest Rating per Category.
     *
     * @param categoryRatings map of category -> max rating
     * @param outputDir       directory to save the PNG
     */
    private static void generateTask1Chart(Map<String, Integer> categoryRatings,
                                            String outputDir) throws IOException {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (Map.Entry<String, Integer> entry : categoryRatings.entrySet()) {
            dataset.addValue(entry.getValue(), "Rating", entry.getKey());
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Highest Rated Product per Category",  // title
                "Category",                             // x-axis label
                "Rating",                               // y-axis label
                dataset,
                PlotOrientation.VERTICAL,
                false,                                  // no legend (single series)
                true,                                   // tooltips
                false                                   // URLs
        );

        // Apply dark theme
        applyDarkTheme(chart);

        // Add subtitle
        TextTitle subtitle = new TextTitle(
                "Maximum customer rating (0-100) per product category",
                SUBTITLE_FONT);
        subtitle.setPaint(new Color(160, 160, 200));
        subtitle.setHorizontalAlignment(HorizontalAlignment.CENTER);
        chart.addSubtitle(subtitle);

        // Customize bars with gradient colors
        CategoryPlot plot = chart.getCategoryPlot();
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBarPainter(new StandardBarPainter()); // flat bars, no 3D effect
        renderer.setMaximumBarWidth(0.08);
        renderer.setDrawBarOutline(false);

        // Assign a vibrant color to each category bar
        int colorIdx = 0;
        for (int i = 0; i < dataset.getColumnCount(); i++) {
            Color base = CATEGORY_COLORS[colorIdx % CATEGORY_COLORS.length];
            GradientPaint gp = new GradientPaint(
                    0f, 0f, base.brighter(),
                    0f, 300f, base.darker());
            renderer.setSeriesPaint(0, base);
            // Since there's one series, color individual items via a custom approach
            colorIdx++;
        }

        // For single-series with different colors per bar, we replace the
        // default renderer with a custom one that paints each bar individually.

        // Use a custom renderer to paint each bar differently
        plot.setRenderer(new BarRenderer() {
            {
                setBarPainter(new StandardBarPainter());
                setMaximumBarWidth(0.08);
                setDrawBarOutline(false);
                setShadowVisible(false);
            }

            @Override
            public java.awt.Paint getItemPaint(int row, int column) {
                return CATEGORY_COLORS[column % CATEGORY_COLORS.length];
            }
        });

        // Rotate category labels slightly if needed
        plot.getDomainAxis().setCategoryLabelPositions(
                CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 6));

        // Set y-axis range
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setRange(0, 105);

        // Save chart as PNG
        File chartFile = new File(outputDir, "highest_rated_per_category.png");
        ChartUtils.saveChartAsPNG(chartFile, chart, 1000, 600);
        System.out.println("Task 1 chart saved: " + chartFile.getAbsolutePath());
    }

    /**
     * Generates the Task 2 grouped bar chart: Distribution by Price &amp; Quantity.
     *
     * @param distribution nested map: price_range -> (quantity_bracket -> count)
     * @param outputDir    directory to save the PNG
     */
    private static void generateTask2Chart(
            Map<String, Map<String, Integer>> distribution,
            String outputDir) throws IOException {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // Build dataset: series = quantity bracket, category = price range
        for (Map.Entry<String, Map<String, Integer>> prEntry : distribution.entrySet()) {
            String priceRange = prEntry.getKey();
            for (Map.Entry<String, Integer> qbEntry : prEntry.getValue().entrySet()) {
                String quantityBracket = qbEntry.getKey();
                int count = qbEntry.getValue();
                dataset.addValue(count, quantityBracket, priceRange);
            }
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Product Distribution: Price Range × Quantity Bracket",
                "Price Range",
                "Number of Products",
                dataset,
                PlotOrientation.VERTICAL,
                true,           // show legend
                true,           // tooltips
                false           // URLs
        );

        // Apply dark theme
        applyDarkTheme(chart);

        // Add subtitle
        TextTitle subtitle = new TextTitle(
                "Low: price<$10 | Medium: $10-$20 | High: price>$20",
                SUBTITLE_FONT);
        subtitle.setPaint(new Color(160, 160, 200));
        subtitle.setHorizontalAlignment(HorizontalAlignment.CENTER);
        chart.addSubtitle(subtitle);

        // Customize bars
        CategoryPlot plot = chart.getCategoryPlot();
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBarPainter(new StandardBarPainter());
        renderer.setDrawBarOutline(false);
        renderer.setMaximumBarWidth(0.15);
        renderer.setItemMargin(0.05);

        // Assign colors to each quantity bracket series
        for (int i = 0; i < dataset.getRowCount(); i++) {
            renderer.setSeriesPaint(i, QUANTITY_COLORS[i % QUANTITY_COLORS.length]);
        }

        // Save chart as PNG
        File chartFile = new File(outputDir, "distribution_by_price_quantity.png");
        ChartUtils.saveChartAsPNG(chartFile, chart, 1000, 600);
        System.out.println("Task 2 chart saved: " + chartFile.getAbsolutePath());
    }

    /**
     * Entry point for chart generation.
     *
     * @param args command-line arguments:
     *             args[0] = Task 1 output directory (containing part-r-00000)
     *             args[1] = Task 2 output directory (containing part-r-00000)
     *             args[2] = Chart output directory (where PNGs will be saved)
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: ChartGenerator <task1_output_dir> <task2_output_dir> <chart_output_dir>");
            System.exit(1);
        }

        String task1Dir  = args[0];
        String task2Dir  = args[1];
        String chartDir  = args[2];

        // Create chart output directory if it doesn't exist
        new File(chartDir).mkdirs();

        System.out.println("========================================");
        System.out.println("Generating Visualization Charts");
        System.out.println("========================================");

        // --- Task 1 Chart ---
        System.out.println("\nReading Task 1 output from: " + task1Dir);
        Map<String, Integer> categoryRatings = readTask1Output(task1Dir);
        System.out.println("Categories found: " + categoryRatings.size());
        for (Map.Entry<String, Integer> e : categoryRatings.entrySet()) {
            System.out.println("  " + e.getKey() + " -> max rating: " + e.getValue());
        }
        generateTask1Chart(categoryRatings, chartDir);

        // --- Task 2 Chart ---
        System.out.println("\nReading Task 2 output from: " + task2Dir);
        Map<String, Map<String, Integer>> distribution = readTask2Output(task2Dir);
        System.out.println("Distribution matrix:");
        for (Map.Entry<String, Map<String, Integer>> prEntry : distribution.entrySet()) {
            for (Map.Entry<String, Integer> qbEntry : prEntry.getValue().entrySet()) {
                System.out.println("  " + prEntry.getKey() + " / " +
                        qbEntry.getKey() + " = " + qbEntry.getValue());
            }
        }
        generateTask2Chart(distribution, chartDir);

        System.out.println("\n========================================");
        System.out.println("Charts generated successfully in: " + chartDir);
        System.out.println("  - highest_rated_per_category.png");
        System.out.println("  - distribution_by_price_quantity.png");
        System.out.println("========================================");
    }
}
