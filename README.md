# BCS 017 Midterm Project-Task 2

## Project Structure
```
BCS017-Midterm/
├── src/com/example/salesanalysis/
│   ├── SalesAnalysisDriver.java      # Driver: orchestrates both MR jobs
│   ├── HighestRatedMapper.java       # Task 1 Mapper
│   ├── HighestRatedCombiner.java     # Task 1 Combiner (local max filter)
│   ├── HighestRatedReducer.java      # Task 1 Reducer
│   ├── DistributionMapper.java       # Task 2 Mapper
│   ├── DistributionCombiner.java     # Task 2 Combiner (local sum)
│   ├── DistributionReducer.java      # Task 2 Reducer
│   ├── EuclideanRecommender.java     # Task 3: Recommendation (Euclidean distance)
│   └── ChartGenerator.java           # Post-processing visualization
├── lib/
│   └── jfreechart-1.5.3.jar         # JFreeChart library
├── build/                            # Compiled .class files
├── output/
│   ├── task1/part-r-00000            # Task 1 raw output
│   ├── task2/part-r-00000            # Task 2 raw output
│   ├── task3/recommend.txt           # Task 3 recommendation result
│   ├── charts/
│   │   ├── highest_rated_per_category.png
│   │   └── distribution_by_price_quantity.png
│   ├── highest_rated_per_category.csv
│   └── distribution_by_price_quantity.csv
├── sales-analysis.jar                # Compiled JAR
├── pom.xml                           # Maven build file (for reference)
├── sales_data.txt                    # Input dataset (12,000 records)
└── sales_data.csv                    # CSV version with headers
```

