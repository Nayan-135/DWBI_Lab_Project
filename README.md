# NYC Taxi Analytics and Fare Prediction using Apache Spark

A big-data project built with **PySpark** on Google Colab. It processes millions of NYC Yellow Taxi trip records using Spark SQL, window functions, in-memory caching and Spark MLlib.

**Author:** <your name>
**Course / Institute:** <your course, college>
**Date:** <date>

---

## 1. Problem Statement

New York City produces millions of taxi trips every month. A single-machine tool struggles with data at this scale, so this project uses Apache Spark to:

1. Clean and process the raw trip data in parallel
2. Find demand patterns (peak hours, busiest zones, busiest days)
3. Predict the taxi fare using machine learning
4. Show the performance benefit of Spark's in-memory caching

---

## 2. Dataset

| Item | Detail |
|---|---|
| Name | NYC TLC Yellow Taxi Trip Records |
| Publisher | NYC Taxi & Limousine Commission (TLC) |
| Format | Parquet (columnar, compressed) |
| Period used | January to March 2024 |
| Size | A few million rows (fill in your actual count) |
| Official page | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |

Direct file links used in the notebook:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
```

Optional zone name lookup (maps location IDs to area names):
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

If a direct link stops working, open the official page above and copy the current Yellow Taxi link.

**Columns used:** pickup and dropoff timestamps, passenger count, trip distance, pickup and dropoff location IDs, payment type, fare amount, tip amount, total amount.

---

## 3. Tools and Technologies

- Apache Spark (PySpark)
- Spark SQL and DataFrame API
- Spark MLlib
- Google Colab
- Python, Matplotlib

---

## 4. Architecture

```
Parquet files (NYC TLC)
        |
        v
Spark Data Source API  -> load and merge months
        |
        v
DataFrame transformations -> cleaning + feature engineering (lazy)
        |
        v
In-memory cache
        |
   +----+---------------------+
   |                          |
   v                          v
Spark SQL analytics      MLlib Pipeline
(GROUP BY, window)       (VectorAssembler + Random Forest)
   |                          |
   v                          v
Charts (Matplotlib)      Fare predictions + metrics
```

---

## 5. Spark Components Used

| Project step | Spark component / concept |
|---|---|
| Start the application | SparkSession |
| Read Parquet files | Data Source API |
| Cleaning and new columns | DataFrame transformations (lazy evaluation) |
| count(), show(), collect() | Actions, which trigger the DAG execution |
| Cache the cleaned data | In-memory persistence |
| Queries on trips | Spark SQL with temporary views |
| Hourly and zone counts | Distributed aggregation (shuffle) |
| Top 3 zones per hour | Window functions (RANK OVER) |
| Query plan | Catalyst optimizer (explain) |
| Feature vector | MLlib VectorAssembler |
| Model | MLlib RandomForestRegressor |
| Workflow | MLlib Pipeline |
| Data split | randomSplit |
| Evaluation | RegressionEvaluator (RMSE, MAE, R2) |
| Charts | Small result collected with toPandas() |

---

## 6. Methodology

### 6.1 Data loading
Each monthly file is read separately, the required columns are selected and cast to fixed types, and the months are merged with `unionByName` to avoid schema mismatch.

### 6.2 Cleaning
- Fare between 0 and 200 dollars
- Trip distance between 0 and 100 miles
- Passenger count greater than 0
- Trip duration between 1 and 180 minutes
- Only trips whose pickup date falls in the loaded months

### 6.3 Feature engineering
Pickup hour, day of week, month, and trip duration derived from the timestamps.

### 6.4 Analytics (Spark SQL)
1. Trips and average fare by hour
2. Top 10 busiest pickup zones
3. Trips by day of week
4. Top 3 zones for every hour (window function)
5. Average tip by distance bucket (card payments)

### 6.5 Machine learning
- **Target:** fare_amount
- **Features:** trip_distance, pickup_hour, pickup_dow, passenger_count, PULocationID, DOLocationID
- **Model:** Random Forest Regressor (30 trees, max depth 8)
- **Split:** 80% train and 20% test on a 30% sample of the cleaned data
- **Note:** trip duration was deliberately left out of the features. Taxi fare is calculated from distance and time, so using duration would leak information and give a misleadingly high score.

### 6.6 Performance demo
The same aggregation query is timed with and without `cache()`, and the Catalyst execution plan is printed with `explain(True)`.

---

## 7. Results

Fill these in from your own notebook output.

### 7.1 Data summary

| Metric | Value |
|---|---|
| Raw rows | ___ |
| Cleaned rows | ___ |
| Rows removed (%) | ___ |
| Partitions | ___ |

### 7.2 Key insights

1. Peak demand hours: ___
2. Busiest pickup zones (location IDs): ___
3. Busiest day of the week: ___
4. Tip behaviour by trip distance: ___

### 7.3 Model performance

| Metric | Value |
|---|---|
| RMSE | ___ |
| MAE | ___ |
| R2 | ___ |

Most important feature: ___

### 7.4 Cache performance

| Run | Time (seconds) |
|---|---|
| Without cache | ___ |
| With cache | ___ |
| Speed-up | ___x |

### 7.5 Screenshots
- Trips by hour chart (`trips_by_hour.png`)
- Top pickup zones chart (`top_zones.png`)
- Model metrics and feature importance output
- `explain(True)` query plan

---

## 8. How to Run

1. Open https://colab.research.google.com and create a new notebook.
2. Paste each code part into its own cell, in order:
   1. Install PySpark and start Spark
   2. Download the data
   3. Load the data
   4. Clean and create features
   5. Spark SQL analytics
   6. Charts
   7. MLlib model
   8. Performance demo
   9. Summary numbers
3. Run the cells from top to bottom.
4. If memory runs out, load only one month (change `months` in the download cell and the month filter in the cleaning cell).

---

## 9. Limitations

- Only 3 months of data are used because of Colab's memory limit.
- Colab runs Spark in local mode on one machine, so the benefit of a true multi-node cluster is not measured.
- Location IDs are used directly, without zone names or coordinates.
- Fare depends heavily on distance, so model accuracy is mostly driven by that one feature.

---

## 10. Future Work

- Run on a real cluster (AWS EMR, Google Dataproc or Databricks) and compare runtimes for 1, 2 and 4 workers
- Use Structured Streaming to replay trips as a live stream and compute demand per zone
- Use GraphFrames to analyse zone-to-zone traffic and run PageRank
- Join the zone lookup table with a broadcast join
- Try Gradient Boosted Trees and hyperparameter tuning with cross-validation
- Predict hourly demand per zone

---

## 11. Conclusion

The project shows how Apache Spark processes a large real-world dataset end to end: loading Parquet, cleaning with lazy transformations, analysing with Spark SQL and window functions, training a model with an MLlib pipeline, and speeding up repeated work with in-memory caching.

---

## 12. References

- Apache Spark documentation: https://spark.apache.org/docs/latest/
- PySpark API reference: https://spark.apache.org/docs/latest/api/python/
- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
