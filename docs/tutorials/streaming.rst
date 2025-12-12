Streaming Pipelines Tutorial
==========================

This tutorial provides a hands-on guide to building and running real-time streaming pipelines with Tauro. While batch pipelines run on a fixed dataset, streaming pipelines operate on unbounded data streams, processing records as they arrive.

Tauro's streaming capabilities are built on top of Spark Structured Streaming, providing a powerful and scalable engine for real-time processing.

Core Concepts
-------------

- **Streaming Readers**: These are sources that provide a continuous stream of data, such as Kafka, Kinesis, or even files in a directory.
- **Streaming Writers**: These are sinks where the processed data is sent, such as the console, another Kafka topic, or files.
- **Triggers**: A trigger defines when the streaming engine should process the next batch of data (e.g., every 10 seconds).
- **Checkpointing**: This is a critical concept for resilience. The streaming engine saves its progress (which data it has processed) to a checkpoint location. If the pipeline fails or is restarted, it can resume from where it left off without losing data.

Example: A File-Based Streaming Pipeline
----------------------------------------

For this tutorial, we'll create a simple but powerful streaming pipeline that:
1.  **Watches** a directory for new CSV files.
2.  **Reads** any new CSV file as a stream of data.
3.  **Transforms** the data (e.g., adds a new column).
4.  **Prints** the transformed data to the console.

This pattern is useful for "drop-folder" style integrations, where other systems deposit files to be processed in real-time.

**1. Configure the Pipeline**

First, let's define the pipeline in YAML. This would typically be in your `config/base/pipelines.yaml` and `nodes.yaml` files.

**`pipelines.yaml`**
.. code-block:: yaml

   file_stream_pipeline:
     nodes:
       - watch_and_transform_files

**`nodes.yaml`**
.. code-block:: yaml

   watch_and_transform_files:
     module: "pipelines.stream_logic.transform_file_stream"
     inputs:
       raw_file_stream: stream_from_input_directory
     outputs:
       - console_output
     runner: spark_streaming # This tells Tauro to use the streaming engine

**`input.yaml`**
.. code-block:: yaml

   stream_from_input_directory:
     type: file
     format: csv
     path: "data/streaming/input" # The directory to watch
     options:
       header: "true"
       inferSchema: "true"

**`output.yaml`**
.. code-block:: yaml

   console_output:
     type: console
     options:
       truncate: "false"

**2. Create the Transformation Logic**

Now, create the Python function that performs the transformation.

**`pipelines/stream_logic.py`**
.. code-block:: python

   from pyspark.sql import DataFrame
   from pyspark.sql.functions import current_timestamp

   def transform_file_stream(raw_file_stream: DataFrame) -> DataFrame:
       """
       Adds a timestamp to the streaming DataFrame.
       """
       return raw_file_stream.withColumn("processing_time", current_timestamp())

**3. Run the Streaming Pipeline**

With the configuration and code in place, you can start the pipeline using the `tauro stream run` command.

.. code-block:: bash

   tauro stream run --pipeline file_stream_pipeline --checkpoint-dir /tmp/tauro_checkpoints

Tauro will start the streaming query. It is now watching the `data/streaming/input` directory.

**4. Test the Stream**

Open a new terminal. Now, let's create a sample CSV file and drop it into the input directory.

**`data/streaming/input/test1.csv`**
.. code-block:: csv

   id,value
   1,A
   2,B
   3,C

As soon as you save this file, you will see output in the console where your pipeline is running. Tauro will process the file and print the transformed data:

.. code-block:: text

   +---+-----+--------------------+
   |id |value|processing_time     |
   +---+-----+--------------------+
   |1  |A    |2024-10-27 12:30:15 |
   |2  |B    |2024-10-27 12:30:15 |
   |3  |C    |2024-10-27 12:30:15 |
   +---+-----+--------------------+

If you drop another file into the directory, it will be processed in the next trigger interval.

**5. Stopping the Pipeline**

Press `Ctrl+C` in the terminal where the pipeline is running to gracefully shut it down. Because we specified a checkpoint directory, if you restart the pipeline, it will know not to re-process `test1.csv`.

Next Steps
----------

- **Explore other sources/sinks**: Change the `input.yaml` and `output.yaml` to use `kafka` instead of `file` and `console`.
- **Complex Logic**: Add more sophisticated transformations in your Python functions.
- **Windowing and Aggregations**: Explore Spark Structured Streaming's capabilities for time-based aggregations.