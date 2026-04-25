package com.example.salesanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class SalesAnalysisDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: SalesAnalysisDriver <input_path> <output_task1> <output_task2>");
            System.err.println("  input_path   : Path to sales_data.txt (local or HDFS)");
            System.err.println("  output_task1 : Output directory for Task 1 (highest rated per category)");
            System.err.println("  output_task2 : Output directory for Task 2 (distribution analysis)");
            return 1;
        }

        String inputPath   = args[0];
        String outputTask1 = args[1];
        String outputTask2 = args[2];

        Configuration conf = getConf();

        // JOB 1: Most Highly-Rated Product per Category
        System.out.println("Starting Job 1: Highest Rated per Category");
        System.out.println("  Input:  " + inputPath);
        System.out.println("  Output: " + outputTask1);

        Job job1 = Job.getInstance(conf, "Sales Analysis - Task 1: Highest Rated per Category");
        job1.setJarByClass(SalesAnalysisDriver.class);

        // Mapper configuration
        job1.setMapperClass(HighestRatedMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // Combiner for local pre-aggregation (reduces mapper output)
        job1.setCombinerClass(HighestRatedCombiner.class);

        // Reducer configuration
        job1.setReducerClass(HighestRatedReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        // Use 1 reducer to ensure output is sorted alphabetically by category
        job1.setNumReduceTasks(1);

        // Input/Output format
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Input/Output paths
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputTask1));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.err.println("ERROR: Job 1 (Highest Rated per Category) failed!");
            return 1;
        }

        System.out.println("Job 1 completed successfully.");
        System.out.println();

        // ================================================================
        // JOB 2: Distribution Analysis by Price Range & Quantity Bracket
        // ================================================================
        System.out.println("========================================");
        System.out.println("Starting Job 2: Distribution Analysis");
        System.out.println("  Input:  " + inputPath);
        System.out.println("  Output: " + outputTask2);
        System.out.println("========================================");

        Job job2 = Job.getInstance(conf, "Sales Analysis - Task 2: Price-Quantity Distribution");
        job2.setJarByClass(SalesAnalysisDriver.class);

        // Mapper configuration
        job2.setMapperClass(DistributionMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // Combiner for local pre-aggregation (sum is associative)
        job2.setCombinerClass(DistributionCombiner.class);

        // Reducer configuration
        job2.setReducerClass(DistributionReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        // Use 1 reducer to get a single, clean output file
        job2.setNumReduceTasks(1);

        // Input/Output format
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Input/Output paths
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputTask2));

        boolean job2Success = job2.waitForCompletion(true);
        if (!job2Success) {
            System.err.println("ERROR: Job 2 (Distribution Analysis) failed!");
            return 1;
        }

        System.out.println("Job 2 completed successfully.");
        System.out.println();
        System.out.println("========================================");
        System.out.println("All jobs completed successfully!");
        System.out.println("  Task 1 output: " + outputTask1);
        System.out.println("  Task 2 output: " + outputTask2);
        System.out.println("========================================");

        // ================================================================
        // TASK 3: Euclidean Recommendation (Standalone, Non-MapReduce)
        // ================================================================
        System.out.println("Starting Task 3: Euclidean Recommendation (Random Sample)");
        String recommendOutput = "output/task3/recommend.txt";
        try {
            com.example.salesanalysis.EuclideanRecommender.main(new String[]{});
            System.out.println("  Task 3 output: " + recommendOutput);
        } catch (Exception e) {
            System.err.println("ERROR: Task 3 (Euclidean Recommendation) failed!");
            e.printStackTrace();
        }
        System.out.println("========================================");
        System.out.println("All jobs (including recommendation) completed!");
        System.out.println("========================================");

        return 0;
    }

    /**
     * Entry point for the MapReduce application.
     *
     * <p>Uses {@link ToolRunner} to parse generic Hadoop options
     * (e.g., {@code -Dfs.defaultFS}, {@code -Dmapreduce.framework.name})
     * before passing remaining arguments to {@link #run(String[])}.</p>
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SalesAnalysisDriver(), args);
        System.exit(exitCode);
    }
}
