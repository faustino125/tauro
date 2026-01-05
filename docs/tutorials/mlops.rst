MLOps Tutorial: Experiment Tracking and Model Registry
====================================================

Tauro provides a production-grade MLOps stack that lets you complement batch pipelines with experiment tracking,
artifact management, monitoring, and optional MLflow integration. This tutorial walks through a complete workflow:

1. Configure the MLOps context and storage backend.
2. Define the pipeline and node that execute the training script.
3. Use ``ExperimentTracker`` and ``ModelRegistry`` to log metrics, artifacts, and register models.
4. Inspect runs, promote models, and monitor health checks.

Prerequisites
-------------

- **Tauro with MLOps extras**: ``pip install tauro[mlops]``
- **Optional MLflow**: ``pip install mlflow`` when you want to mirror runs in MLflow.
- **ML framework**: ``pip install scikit-learn`` for the sample training script.
- **Storage backend**: Local filesystem or Databricks reachable for artifacts.

What You'll Learn
-----------------

By the end of this tutorial, you'll know how to:

- Configure Tauro's MLOps layer for experiment tracking
- Define ML training pipelines with inputs and outputs
- Use Tauro's MLOps helpers to log parameters, metrics, and models
- Register trained models to a production registry
- Execute and monitor training runs
- View results in MLflow (optional)

Step 1: Configure Tauro's MLOps Layer
--------------------------------------

Tauro discovers configuration through the descriptor file (``settings_json.json`` by default) that maps each 
environment to the five core configuration files. Add an ``mlops`` block to your ``global_settings.yaml`` to tell 
Tauro where to store experiments, runs, and artifacts.

**Create the descriptor** (``settings_json.json``):

.. code-block:: json

   {
     "base_path": ".",
     "env_config": {
       "base": {
         "global_settings_path": "config/global_settings.yaml",
         "pipelines_config_path": "config/pipelines.yaml",
         "nodes_config_path": "config/nodes.yaml",
         "input_config_path": "config/input.yaml",
         "output_config_path": "config/output.yaml"
       },
       "dev": {
         "global_settings_path": "config/dev/global_settings.yaml",
         "input_config_path": "config/dev/input.yaml",
         "output_config_path": "config/dev/output.yaml"
       }
     }
   }

**Add MLOps configuration** (``config/base/global_settings.yaml``):

.. code-block:: yaml

   mlops:
     backend_type: local             # local filesystem (default) or databricks
     storage_path: ./mlops_data      # root for experiments, registry, and logs
     model_retention_days: 90
     default_experiment_name: customer_churn
     experiment_tracking:
       metric_buffer_size: 200       # buffer metrics to avoid OOM
       auto_flush_metrics: true      # flush on threshold
     registry:
       max_versions_per_model: 100
     mlflow:
       tracking_uri: http://127.0.0.1:5000    # optional: MLflow server
       experiment_name: customer_churn_ml

**For Databricks deployments**, use environment variables instead:

.. code-block:: bash

   export TAURO_MLOPS_BACKEND=databricks
   export TAURO_MLOPS_CATALOG=ml_catalog
   export TAURO_MLOPS_SCHEMA=experiments
   export DATABRICKS_HOST=https://workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=dapi...

.. note::
   
   Tauro does not automatically source ``.env`` files. Load environment variables manually via ``python-dotenv`` 
   before running ``tauro`` or instantiating ``AppConfigManager``.

Step 2: Define the Training Pipeline
-------------------------------------

Create a pipeline that will execute your training script. This requires defining both the pipeline and the node.

**Define the pipeline** (``config/base/pipelines.yaml``):

.. code-block:: yaml

   ml_training_pipeline:
     description: "Train and register customer churn prediction model"
     nodes:
       - train_churn_model

**Define the node** (``config/base/nodes.yaml``):

.. code-block:: yaml

   nodes:
     train_churn_model:
       function: "pipelines.ml.train_model.train_and_register"
       description: "Train RandomForest model and register to registry"
       inputs:
         training_data: preprocessed_training_data  # Defined in input.yaml

**Define the data source** (``config/base/inputs.yaml``):

.. code-block:: yaml

   inputs:
     preprocessed_training_data:
       path: "data/train.parquet"
       format: "parquet"
       schema:
         - name: churn
           type: boolean
         - name: age
           type: integer
         - name: account_length
           type: integer
         # ... other features

Step 3: Create the Training Script
----------------------------------

This is where the ML logic lives. Tauro automatically injects ``ExperimentTracker`` and ``ModelRegistry`` objects
based on type hints in your function signature.

**Create the training module** (``pipelines/ml/train_model.py``):

.. code-block:: python

   import pandas as pd
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.model_selection import train_test_split
   from sklearn.metrics import accuracy_score, f1_score, confusion_matrix

   from tauro.mlops import ExperimentTracker, ModelRegistry

   def train_and_register(
       training_data: pd.DataFrame,
       tracker: ExperimentTracker,
       registry: ModelRegistry,
   ):
       """
       Train a customer churn prediction model.
       
       Tauro automatically injects the 'tracker' and 'registry' objects
       when they are type-hinted in the function signature.
       
       Parameters:
           training_data: DataFrame with training features and target
           tracker: ExperimentTracker for logging runs
           registry: ModelRegistry for registering models
       """
       
       print("Starting customer churn model training...")

       # 1. Prepare data
       X = training_data.drop('churn', axis=1)
       y = training_data['churn']
       X_train, X_test, y_train, y_test = train_test_split(
           X, y, test_size=0.2, random_state=42
       )
       print(f"Training set: {len(X_train)} samples | Test set: {len(X_test)} samples")

       # 2. Start an experiment run
       # Everything logged inside this context is grouped together
       with tracker.start_run(run_name="random_forest_baseline") as run:
           
           # 3. Log hyperparameters
           params = {
               "model": "RandomForestClassifier",
               "n_estimators": 150,
               "max_depth": 12,
               "min_samples_leaf": 4,
               "random_state": 42
           }
           run.log_params(params)
           print(f"✓ Logged parameters: {params}")

           # 4. Train the model
           model = RandomForestClassifier(**{k: v for k, v in params.items() if k != 'model'})
           model.fit(X_train, y_train)
           print("✓ Model training complete")
           
           # 5. Evaluate on test set
           y_pred = model.predict(X_test)
           accuracy = accuracy_score(y_test, y_pred)
           f1 = f1_score(y_test, y_pred, average='weighted')
           tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
           
           # 6. Log evaluation metrics
           metrics = {
               "accuracy": accuracy,
               "f1_score": f1,
               "true_positives": int(tp),
               "false_positives": int(fp),
               "true_negatives": int(tn),
               "false_negatives": int(fn),
           }
           run.log_metrics(metrics)
           print(f"✓ Logged metrics: accuracy={accuracy:.3f}, f1={f1:.3f}")
           
           # 7. Log model artifact
           model_path = "./models/churn_model.pkl"
           run.log_model(model, artifact_path=model_path)
           print(f"✓ Logged model artifact to {model_path}")

           # 8. Register model if it meets quality threshold
           if accuracy > 0.85:
               print(f"✓ Accuracy ({accuracy:.3f}) exceeds threshold (0.85)")
               model_info = registry.register_model(
                   model=model,
                   model_name="churn-predictor",
                   description="RandomForest classifier for customer churn prediction",
                   metrics=metrics,
                   tags={
                       "project": "customer_churn",
                       "framework": "sklearn",
                       "stage": "production-candidate"
                   }
               )
               print(f"✅ Model registered: {model_info.name} v{model_info.version}")
           else:
               print(f"⚠ Accuracy ({accuracy:.3f}) below threshold (0.85) - model not registered")
               return {"status": "trained_but_not_registered", "accuracy": accuracy}
       
       return {"status": "success", "accuracy": accuracy, "f1": f1}

**Understanding the code:**

- **Type hints for injection**: The ``tracker: ExperimentTracker`` and ``registry: ModelRegistry`` parameters tell 
  Tauro's executor to initialize and inject these objects automatically.
- **Context manager**: The ``with tracker.start_run(...)`` block groups all logs under a single run.
- **Metrics buffering**: Metrics are buffered in memory (controlled by ``metric_buffer_size``) then flushed to disk
  when the buffer fills or the run ends.
- **Model registration**: Only models exceeding the accuracy threshold are registered to the production registry.

Step 4: Run the Pipeline via CLI
---------------------------------

Validate and run the pipeline:

.. code-block:: bash

   # Validate configuration
   tauro run --env dev --pipeline ml_training_pipeline --validate-only
   
   # Execute the pipeline
   tauro run --env dev --pipeline ml_training_pipeline

You should see output like:

.. code-block:: text

   ✓ Loading configuration...
   ✓ Validating pipeline...
   ✓ Starting execution...
   ✓ train_churn_model .................... [1/1]
   Starting customer churn model training...
   Training set: 4000 samples | Test set: 1000 samples
   ✓ Logged parameters: {...}
   ✓ Model training complete
   ✓ Logged metrics: accuracy=0.892, f1=0.885
   ✓ Logged model artifact to ./models/churn_model.pkl
   ✓ Accuracy (0.892) exceeds threshold (0.85)
   ✅ Model registered: churn-predictor v1
   ✓ Pipeline completed in 45 seconds

Step 5: View Results in MLflow (Optional)
------------------------------------------

If you configured MLflow in Step 1, Tauro mirrors all runs to the MLflow server.

1. **Start MLflow UI** (if not already running):

   .. code-block:: bash

      mlflow ui

2. **Open the UI** at http://127.0.0.1:5000

3. **Explore your experiment**:

   - Go to the "customer_churn_ml" experiment
   - Click on the "random_forest_baseline" run
   - View parameters, metrics, and artifacts
   - In the "Models" section, find "churn-predictor"
   - Inspect registered model versions and metadata

Step 6: Programmatic Execution
-------------------------------

Execute the pipeline from Python code for integration with Airflow, Jupyter, or other applications:

.. code-block:: python

   from tauro import ContextLoader, PipelineExecutor
   from tauro.cli.config import AppConfigManager

   # Load configuration
   descriptor = AppConfigManager("settings_json.json")
   config_paths = descriptor.get_env_config("dev")
   context = ContextLoader().load_from_paths(config_paths, "dev")
   
   # Execute pipeline
   executor = PipelineExecutor(context)
   try:
       executor.run_pipeline("ml_training_pipeline")
       print("✅ Pipeline completed successfully")
   except Exception as e:
       print(f"❌ Pipeline failed: {e}")

How Tauro's MLOps Works
------------------------

**Automatic Helper Injection**

Type hints such as ``tracker: ExperimentTracker`` tell Tauro's executor to:

1. Initialize the helper object with the correct configuration
2. Scope it to the current run
3. Inject it automatically when the function is called

This eliminates boilerplate and keeps your ML code focused on training, not configuration.

**Buffered Metrics Logging**

To avoid memory spikes when logging thousands of metrics:

- Metrics are collected in a buffer (size: ``metric_buffer_size``)
- Buffer is flushed when full or when the run ends
- ``auto_flush_metrics`` controls automatic flushing

**Registry Guarantees**

Model registration ensures:

- Artifacts are copied to ``mlops_data/model_registry/artifacts/<model>/<version>/``
- Metadata (tags, metrics, description) is saved alongside
- File locks prevent corruption during parallel registrations
- Versions are tracked for auditing and rollback

**MLflow Integration** (Optional)

If MLflow is configured, Tauro:

- Mirrors runs to the external MLflow tracking server
- Synchronizes parameters, metrics, and artifacts
- Registers models in MLflow's model registry
- Maintains consistency across both systems

Observability and Monitoring
-----------------------------

Tauro exposes health checks and metrics for production monitoring:

**Health Checks**

.. code-block:: python

   from tauro.mlops import get_health_monitor
   
   monitor = get_health_monitor()
   monitor.register_storage_check()  # Check disk space
   monitor.check_all()               # Run all checks

**Metrics Collection**

.. code-block:: python

   from tauro.mlops import get_metrics_collector
   
   collector = get_metrics_collector()
   print(collector.get_counter("models_registered"))
   print(collector.get_counter("active_runs"))

**Event Streaming**

.. code-block:: python

   from tauro.mlops import get_event_emitter
   
   emitter = get_event_emitter()
   emitter.on_event(event_type="MODEL_REGISTERED", callback=my_handler)

Troubleshooting
---------------

**Problem: OOM while logging metrics**

   Reduce ``metric_buffer_size`` in ``global_settings.yaml`` or call ``run.flush_metrics()`` explicitly.

**Problem: Artifacts missing**

   Ensure artifact paths are absolute or relative to your working directory. Tauro's ``PathValidator`` rejects 
   ``..`` segments for security.

**Problem: Databricks authentication fails**

   Verify ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` environment variables are set and correct.

**Problem: Model registry locked**

   If multiple processes register the same model simultaneously, retry after a short delay. File locks prevent 
   corruption but require application-side retry logic.

**Problem: MLflow runs not appearing**

   Check that ``tracking_uri`` in ``global_settings.yaml`` points to your MLflow server. Ensure the server is 
   accessible and running.

Next Steps
----------

- :doc:`/getting_started` to apply the same configuration flow to non-ML workloads.
- :doc:`/library_usage` to embed Tauro executions within other Python services (Airflow, Prefect, etc.).
- :doc:`/advanced/troubleshooting` for deeper diagnostics on storage, concurrency, and configuration.
- :doc:`/tutorials/streaming` to keep your training data fresh with streaming ETLs.