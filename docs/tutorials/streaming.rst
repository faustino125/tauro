Streaming Pipelines Tutorial
=============================

This tutorial shows you how to build real-time data pipelines with Tauro. While batch pipelines process fixed datasets on a schedule, streaming pipelines process data continuously as it arrives.

**Best for**: Real-time data processing, event streams, continuous monitoring, live dashboards.

**Built on**: Apache Spark Structured Streaming for scalable, fault-tolerant processing.

When to Use Streaming
---------------------

Use streaming pipelines when you need:

- ✅ Real-time data processing (not hourly or daily batches)
- ✅ Continuous event processing (Kafka, IoT sensors, click streams)
- ✅ Low-latency results (minutes or seconds, not hours)
- ✅ Always-on processing (not scheduled jobs)

Use batch pipelines when you need:

- ✅ Scheduled processing (daily ETL, weekly reports)
- ✅ Processing large historical datasets
- ✅ Complex data transformations
- ✅ Simpler setup and testing

Key Concepts
-----------

**Data Source** (Input)
   Where streaming data comes from: Kafka topics, file directories, cloud message queues, databases with change data capture.

**Transformation**
   Your Python code that processes each record or micro-batch of records as it arrives.

**Output** (Sink)
   Where processed results go: another Kafka topic, database, files, dashboards, alerts.

**Trigger**
   How often Tauro processes new data. Options:
   - Every N seconds (e.g., process every 5 seconds)
   - As soon as data arrives (micro-batches)
   - Manual trigger

**Checkpoint**
   Tauro saves its progress so if the pipeline crashes, it can resume without losing or re-processing data.

Simple Streaming Example: Monitor a Folder
-------------------------------------------

Let's build a real-time pipeline that watches a folder, processes CSV files as they arrive, and saves results.

**What will happen:**
1. Tauro watches ``data/input/`` for new CSV files
2. When a file arrives, it's processed immediately
3. Results are written to ``data/output/``
4. Pipeline continues running, waiting for more files

**Step 1: Configure Your Pipeline**

Create ``config/pipelines.yaml``:

.. code-block:: yaml

   real_time_sales:
     nodes: [process_sales_stream]

Create ``config/nodes.yaml``:

.. code-block:: yaml

   process_sales_stream:
     function: "src.nodes.process_sales"

Create ``config/inputs.yaml``:

.. code-block:: yaml

   inputs:
     sales_stream:
       path: data/input/
       format: csv
       streaming: true  # Enable streaming mode

Create ``config/outputs.yaml``:

.. code-block:: yaml

   outputs:
     processed_sales:
       path: data/output/
       format: parquet
       streaming: true

**Step 2: Write Your Processing Code**

Create ``src/nodes/process_sales.py``:

.. code-block:: python

   def process_sales(df):
       """Process incoming sales data."""
       # Add processing timestamp
       import pyspark.sql.functions as F
       df = df.withColumn(
           "processed_at",
           F.current_timestamp()
       )
       
       # Only keep valid sales
       df = df.filter(df['amount'] > 0)
       
       # Add day of week
       df = df.withColumn(
           "day_of_week",
           F.dayofweek(F.col("date"))
       )
       
       return df

**Step 3: Run Your Streaming Pipeline**

Start the pipeline:

.. code-block:: bash

   tauro --env dev --pipeline real_time_sales --stream

The pipeline will start running and wait for data to arrive.

**Step 4: Test With Sample Data**

Open a new terminal and create a test file:

.. code-block:: bash

   # Create sample sales data
   mkdir -p data/input
   cat > data/input/sales_2024_01_15.csv << 'EOF'
   id,date,amount,customer
   1,2024-01-15,150.00,Alice
   2,2024-01-15,200.00,Bob
   3,2024-01-15,75.50,Charlie
   EOF

Within a few seconds, your pipeline will process this file! Check the output:

.. code-block:: bash

   # Check processed files
   ls data/output/
   
   # View the results
   parquet-tools show data/output/part-*.parquet

**Step 5: Add More Data**

Create another file while the pipeline is running:

.. code-block:: bash

   cat > data/input/sales_2024_01_16.csv << 'EOF'
   id,date,amount,customer
   4,2024-01-16,300.00,David
   5,2024-01-16,125.00,Eve
   EOF

Your pipeline automatically processes it in seconds!

**Step 6: Stop the Pipeline**

Press ``Ctrl+C`` to gracefully shut down the pipeline. Thanks to checkpointing, if you restart it, it won't re-process old files.

.. code-block:: bash

   # Files already processed are tracked
   # Restarting the pipeline skips them
   tauro --env dev --pipeline real_time_sales --stream

Next Steps
----------

- **Explore other sources/sinks**: Change the `input.yaml` and `output.yaml` to use `kafka` instead of `file` and `console`.
- **Complex Logic**: Add more sophisticated transformations in your Python functions.
- **Windowing and Aggregations**: Explore Spark Structured Streaming's capabilities for time-based aggregations.