Troubleshooting & Solutions
===========================

Something not working? Here are solutions to the most common problems.

Quick Diagnosis
----------------

When a pipeline fails, follow these steps:

.. code-block:: bash

   # 1. Run with detailed logging
   tauro run --env dev --pipeline my_pipeline --log-level DEBUG

   # 2. Look for the error message
   # 3. Use the table below to find your error
   # 4. Follow the solution

Common Problems & Solutions
----------------------------

"Pipeline Not Found"
~~~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: Pipeline 'my_pipeline' not found

**Causes & Solutions:**

1. **Wrong pipeline name:**

   .. code-block:: bash

      # List all pipelines
      tauro config list-pipelines --env dev

      # Check that your pipeline name exactly matches
      tauro run --env dev --pipeline correct_name

2. **Configuration file not loaded:**

   .. code-block:: bash

      # Make sure pipelines.yaml exists
      ls -la config/pipelines.yaml

      # If it doesn't exist, create it:
      cat > config/pipelines.yaml << EOF
      pipelines:
        my_pipeline:
          nodes: [extract, load]
      EOF

"Configuration Invalid"
~~~~~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: Configuration validation error:
   - Missing required key: 'input_path'
   - Unknown key: 'invalid_field'

**Solutions:**

1. **Fix YAML syntax** (YAML is sensitive to spacing):

   .. code-block:: yaml

      # ✗ Wrong (mixed spaces/tabs)
      pipelines:
       my_pipeline:
        nodes: [a, b]

      # ✓ Right (consistent 2-space indent)
      pipelines:
        my_pipeline:
          nodes: [a, b]

2. **Add missing required fields:**

   .. code-block:: yaml

      # global.yaml must have:
      input_path: /data/input
      output_path: /data/output
      log_level: INFO

3. **Check for typos:**

   .. code-block:: bash

      # Use a YAML validator (optional)
      python -m yaml config/global.yaml

"Node Function Not Found"
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: Could not find function 'src.nodes.extract'

**Solutions:**

1. **Check file exists:**

   .. code-block:: bash

      ls -la src/nodes/extract.py
      # If missing, create it

2. **Check function is defined:**

   .. code-block:: bash

      # If file exists, check for function
      grep "def extract" src/nodes/extract.py

3. **Fix the path in config:**

   .. code-block:: yaml

      # config/nodes.yaml
      extract:
        function: "src.nodes.extract"  # File: src/nodes/extract.py
                                        # Function: def extract()

4. **Import test:**

   .. code-block:: bash

      # Test Python import
      python -c "from src.nodes.extract import extract; print('OK')"
      # If error, fix the import

"Data Not Found"
~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: File not found: data/input/sales.csv

**Solutions:**

1. **Check file exists:**

   .. code-block:: bash

      ls -la data/input/sales.csv

2. **Use correct path:**

   .. code-block:: yaml

      # inputs.yaml
      inputs:
        raw_data:
          path: data/input/sales.csv  # Relative to project root

      # Or absolute path:
          path: /home/user/project/data/input/sales.csv

3. **Create test data if missing:**

   .. code-block:: bash

      mkdir -p data/input
      echo "id,name,amount
      1,Alice,100
      2,Bob,200" > data/input/sample.csv

4. **Check permissions:**

   .. code-block:: bash

      # Make sure you can read the file
      cat data/input/sales.csv | head
      # Should show your data

"Pipeline Is Very Slow"
~~~~~~~~~~~~~~~~~~~~~~~

**Solutions:**

1. **Check what's slow:**

   .. code-block:: bash

      tauro run --env dev --pipeline my_pipeline --log-level DEBUG

      # Look for timestamps to see which step is slow
      # Example output:
      # 14:32:15 - Starting node 'extract'
      # 14:32:20 - Completed (5 seconds)  ← Fast
      # 14:32:20 - Starting node 'transform'
      # 14:35:00 - Completed (160 seconds) ← Slow!

2. **Optimize the slow step:**

   .. code-block:: python

      # If transform is slow, optimize your Python code
      # Bad:
      def bad_transform(df):
          result = []
          for idx, row in df.iterrows():  # ✗ Very slow
              result.append(row * 2)
          return pd.DataFrame(result)

      # Good:
      def good_transform(df):
          return df * 2  # ✓ Fast (vectorized)

3. **Use smaller data for testing:**

   .. code-block:: yaml

      # During development, use less data
      inputs:
        test_data:
          path: data/input/sample_100_rows.csv  # Not full dataset

4. **Increase workers (if nodes are independent):**

   .. code-block:: yaml

      # config/global.yaml
      max_workers: 8  # Up from default 4

5. **Use Spark for big data:**

   .. code-block:: bash

      pip install tauro[spark]

"Out of Memory"
~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: MemoryError: Cannot allocate memory

**Solutions:**

1. **Process in chunks:**

   .. code-block:: python

      # ✗ Don't load everything
      df = pd.read_csv("huge_file.csv")

      # ✓ Process in chunks
      for chunk in pd.read_csv("huge_file.csv", chunksize=10000):
          process_chunk(chunk)

2. **Use Parquet (more efficient):**

   .. code-block:: yaml

      inputs:
        # ✗ CSV uses lots of memory
        raw_data:
          path: data/input/sales.csv
          format: csv

      # ✓ Parquet is more efficient
      outputs:
        processed:
          path: data/output/results.parquet
          format: parquet

3. **Filter data early:**

   .. code-block:: python

      def extract(df):
          """Get data and filter early."""
          # Only keep what you need
          df = df[df['amount'] > 0]
          df = df[['id', 'amount', 'date']]
          return df

4. **Increase available memory:**

   .. code-block:: yaml

      # config/global.yaml (if using Spark)
      spark:
        driver_memory: "8g"    # Up from "4g"
        executor_memory: "8g"  # Up from "4g"

"Python Package Not Found"
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: ModuleNotFoundError: No module named 'pandas'

**Solutions:**

1. **Install missing package:**

   .. code-block:: bash

      pip install pandas

2. **Update requirements.txt:**

   .. code-block:: bash

      pip freeze > requirements.txt

3. **Use virtual environment:**

   .. code-block:: bash

      # Create environment
      python -m venv venv

      # Activate it
      source venv/bin/activate  # Mac/Linux
      # or
      venv\Scripts\activate     # Windows

      # Install packages
      pip install -r requirements.txt

"Environment Variable Not Found"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: Environment variable 'DATABASE_URL' not found

**Solutions:**

1. **Create .env file:**

   .. code-block:: bash

      # Create .env in project root
      echo "DATABASE_URL=postgresql://localhost/mydb" > .env
      echo "API_KEY=abc123" >> .env

      # Add to .gitignore (don't commit secrets!)
      echo ".env" >> .gitignore

2. **Or export variable:**

   .. code-block:: bash

      export DATABASE_URL="postgresql://localhost/mydb"
      tauro run --env dev --pipeline my_pipeline

3. **Or set in config:**

   .. code-block:: yaml

      # config/global.yaml
      database_url: ${DATABASE_URL}  # From environment

"Permission Denied"
~~~~~~~~~~~~~~~~~~~

**Error Message:**

.. code-block:: text

   ERROR: Permission denied: /data/output/results.parquet

**Solutions:**

1. **Check directory exists:**

   .. code-block:: bash

      mkdir -p data/output

2. **Check permissions:**

   .. code-block:: bash

      # Check if directory is writable
      touch data/output/test.txt
      rm data/output/test.txt

3. **Fix permissions (Linux/Mac):**

   .. code-block:: bash

      chmod 755 data/output

"Different Results in Dev vs Prod"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Problem:** Pipeline works in dev but fails in prod

**Solutions:**

1. **Test in staging first:**

   .. code-block:: bash

      # Test in staging before prod
      tauro run --env staging --pipeline my_pipeline

2. **Use same data:**

   .. code-block:: yaml

      # Make sure dev and prod use similar data structures
      # Check: column names, data types, value ranges

3. **Check environment overrides:**

   .. code-block:: yaml

      # List what's different between environments
      diff config/dev/global.yaml config/prod/global.yaml

4. **Add validation:**

   .. code-block:: python

      def validate_input(df):
          """Ensure data is what we expect."""
          expected_cols = ['id', 'amount', 'date']
          missing = [c for c in expected_cols if c not in df.columns]
          if missing:
              raise ValueError(f"Missing columns: {missing}")
          return df

Debugging Techniques
--------------------

**Print Statements for Debugging**

.. code-block:: python

   def transform(df):
       """Transform with debugging."""
       print(f"Input shape: {df.shape}")
       print(f"Columns: {df.columns.tolist()}")
       print(f"First few rows:\n{df.head()}")

       df = df[df['amount'] > 0]

       print(f"Output shape: {df.shape}")
       print(f"Removed {len(df)} rows")

       return df

**Run with Verbose Logging**

.. code-block:: bash

   tauro run --env dev --pipeline my_pipeline --log-level DEBUG

**Test Node Independently**

.. code-block:: bash

   # Test a Python function directly
   python -c "
   from src.nodes.extract import extract
   df = extract()
   print(df.head())
   print(df.shape)
   print(df.info())
   "

**Check Intermediate Results**

.. code-block:: bash

   # Save intermediate data during testing
   # In your node:
   df.to_csv('debug_output.csv')
   # Then inspect the CSV file

**Validate Configuration**

.. code-block:: bash

   # Before running, validate everything
   tauro run --env dev --pipeline my_pipeline --validate-only

Getting More Help
-----------------

If none of these solutions work:

1. **Check logs:**

   .. code-block:: bash

      # Save full log
      tauro run --env dev --pipeline my_pipeline --log-level DEBUG > debug.log 2>&1
      cat debug.log | grep ERROR

2. **Search GitHub issues:**

   https://github.com/faustino125/tauro/issues

3. **Open an issue:**

   Include:
   - Your Tauro version: ``tauro --version``
   - Error message (full output)
   - Your configuration files (remove secrets)
   - Python version: ``python --version``

4. **Ask the community:**

   - GitHub Discussions
   - Stack Overflow (tag: tauro-framework)

5. **Contact support:**

   Email: support@tauro-framework.com

Summary Table
~~~~~~~~~~~~~

| Problem | Check This First |
|---------|------------------|
| Pipeline not found | ``tauro config list-pipelines --env dev`` |
| Config error | YAML indentation and syntax |
| Node not found | File exists and function name is correct |
| Data not found | File path is correct and file exists |
| Permission denied | Directory exists and is writable |
| Out of memory | Process in chunks, use Parquet |
| Environment vars | Check ``.env`` file exists |
| Different results | Check environment overrides |
| Slow pipeline | Run with DEBUG logging to find slow step |

Prevention Tips
---------------

✅ **Always test locally before running in production**

.. code-block:: bash

   tauro run --env dev --pipeline my_pipeline

✅ **Validate configuration before deploying**

.. code-block:: bash

   tauro run --env prod --pipeline my_pipeline --validate-only

✅ **Use version control**

.. code-block:: bash

   git add config/ src/
   git commit -m "Add new pipeline"

✅ **Document your pipelines**

.. code-block:: text

   # In your README
   - What each pipeline does
   - When it runs
   - What data it needs
   - What can go wrong

✅ **Monitor in production**

.. code-block:: bash

   # Save logs
   tauro run --env prod --pipeline my_pipeline 2>&1 | tee logs/run.log

   # Check for errors
   grep ERROR logs/run.log

Next Steps
----------

- :doc:`guides/batch_etl` - Build a complete pipeline
- :doc:`guides/configuration` - Master configuration
- :doc:`best_practices` - Learn best practices
