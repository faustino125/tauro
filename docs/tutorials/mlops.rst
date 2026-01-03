MLOps Tutorial: Experiment Tracking and Model Registry
====================================================

Tauro bundles a production-ready MLOps stack that couples pipelines with experiment tracking, artifact storage,
monitoring, and optional MLflow mirroring. This tutorial walks through configuring the stack, writing the training
node, and executing the workflow both from the CLI and programmatically.

Prerequisites
-------------

- ``pip install tauro[mlops]`` to install the MLOps helpers.
- Optional: ``pip install mlflow`` when you want Tauro to mirror runs to an MLflow tracking URI.
- ``pip install scikit-learn`` (or another ML library) for the training script.
- A storage backend (local filesystem, Databricks Unity Catalog, or network share) reachable for metrics and
  artifacts.

Configuring Tauro's MLOps Layer
-------------------------------

Tauro discovers configuration through the descriptor file (``settings_json.json`` by default) that maps every
environment to the five core YAML/JSON/DSL files (``global_settings.*``, ``pipelines.*``, ``nodes.*``, ``input.*``,
``output.*``). The CLI and ``AppConfigManager`` both read that descriptor before instantiating the context. A
minimal descriptor looks like:

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

The descriptor must live next to the ``settings_*`` files at your project root. Tauro requires ``env_config`` to
define the key paths, but you can add custom entries such as ``mlops_config_path`` if your project keeps helpers
in additional files. See :doc:`configuration` for the full descriptor contract and environment patches.

In ``global_settings.*`` add an ``mlops`` block that points Tauro to the artifact and registry roots:

.. code-block:: yaml

   mlops:
     backend_type: local             # local filesystem (default) or databricks
     storage_path: ./mlops_data      # root for experiments, registry, and logs
     model_retention_days: 90
     default_experiment_name: customer_churn
     experiment_tracking:
       metric_buffer_size: 200
       auto_flush_metrics: true
     registry:
       max_versions_per_model: 100
     mlflow:
       tracking_uri: http://127.0.0.1:5000
       experiment_name: customer_churn_ml

For Databricks deployments, export the secrets instead of checking them into YAML:

.. code-block:: bash

   export TAURO_MLOPS_BACKEND=databricks
   export TAURO_MLOPS_CATALOG=ml_catalog
   export TAURO_MLOPS_SCHEMA=experiments
   export DATABRICKS_HOST=https://workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=dapi...

Tauro does not automatically ``source`` a ``.env`` file. Load environment variables manually or via ``python-dotenv``
before running ``tauro`` or instantiating ``AppConfigManager``.

Define the Training Pipeline
----------------------------

Register the pipeline and the node that executes your ML training function:

.. code-block:: yaml

   ml_training_pipeline:
     nodes:
       - train_churn_model

.. code-block:: yaml

   train_churn_model:
     module: pipelines.ml.train_model.train_and_register
     inputs:
       training_data: preprocessed_training_data
     runner: local

Writing the Training Script
---------------------------

Tauro inspects the function signature and injects helpers that are type-hinted. Import the helpers from
``tauro.mlops`` and let the executor manage their lifecycle:

.. code-block:: python

   import pandas as pd
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.model_selection import train_test_split
   from sklearn.metrics import accuracy_score, f1_score

   from tauro.mlops import ExperimentTracker, ModelRegistry

   def train_and_register(
       training_data: pd.DataFrame,
       tracker: ExperimentTracker,
       registry: ModelRegistry,
   ):
       """Train the churn model, log the experiment, and register the artifact."""

       X = training_data.drop("churn", axis=1)
       y = training_data["churn"]
       X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

       with tracker.start_run(run_name="random_forest_baseline") as run_id:
           params = {
               "n_estimators": 150,
               "max_depth": 12,
               "min_samples_leaf": 4,
               "random_state": 42,
           }
           tracker.log_params(run_id, params)

           model = RandomForestClassifier(**params)
           model.fit(X_train, y_train)

           y_pred = model.predict(X_test)
           accuracy = accuracy_score(y_test, y_pred)
           f1 = f1_score(y_test, y_pred, average="weighted")

           tracker.log_metrics(run_id, {"accuracy": accuracy, "f1_score": f1})
           artifact_path = "./models/churn_model.pkl"
           tracker.log_artifact(run_id, artifact_path)

           if accuracy > 0.88:
               registry.register_model(
                   name="churn-predictor",
                   artifact_path=artifact_path,
                   description="Baseline churn classifier",
                   metrics={"accuracy": accuracy},
                   tags={"project": "churn", "framework": "sklearn"},
                   experiment_run_id=run_id,
               )

How it Works
------------

- **Automatic helper injection**: Type hints such as ``tracker: ExperimentTracker`` let the executor provide ready-to-use
  objects scoped to the active run.
- **Buffered logging**: ``metric_buffer_size`` batches metrics while ``auto_flush_metrics`` flushes only when the
  buffer is full so you avoid OOM spikes.
- **Registry guarantees**: Model registration copies artifacts into
  ``mlops_data/model_registry/artifacts/<model>/<version>`` and records stage metadata. File locks guard parallel
  registries.
- **MLflow bridging**: When ``mlflow`` is enabled, Tauro mirrors the same runs to the tracking URI maintained in
  ``global_settings``.

Run the Pipeline via CLI
------------------------

Validate and run the pipeline with the usual commands:

.. code-block:: bash

   tauro run --env dev --pipeline ml_training_pipeline --validate-only
   tauro run --env dev --pipeline ml_training_pipeline

After completion, inspect ``mlops_data/experiment_tracking`` or your MLflow UI (``http://127.0.0.1:5000``) to
review parameters, metrics, artifacts, and registered models. Event listeners surface ``EventType.MODEL_REGISTERED``
through ``core.mlops.events`` for downstream automation.

Programmatic Execution
----------------------

Re-use the same descriptor outside the CLI: ``AppConfigManager`` drives environment resolution, ``ContextLoader``
builds the context, and ``PipelineExecutor`` runs the pipeline.

.. code-block:: python

   from core.cli.config import AppConfigManager
   from core.config.context_loader import ContextLoader
   from core.exec.executor import PipelineExecutor

   descriptor = AppConfigManager("config/settings_json.json")
   config_paths = descriptor.get_env_config("dev")
   context = ContextLoader().load_from_paths(config_paths, "dev")
   executor = PipelineExecutor(context)
   executor.run_pipeline("ml_training_pipeline")

Batch pipelines return ``None`` when they finish successfully, while streaming pipelines return an execution ID from
``run_streaming_pipeline``. Use ``executor.get_streaming_pipeline_status`` and ``executor.stop_streaming_pipeline``
when you orchestrate long-running jobs.

Observability and Monitoring
----------------------------

- ``get_health_monitor()`` lets you register ``StorageHealthCheck`` or ``DiskHealthCheck`` to surface quota issues
  before they impact runs.
- ``get_metrics_collector()`` emits operational counters such as ``models_registered`` and ``active_runs`` for
  Prometheus or StatsD.
- ``get_event_emitter()`` streams ``RUN_COMPLETED`` and ``MODEL_REGISTERED`` events into Slack, Datadog, or your
  own sinks.

Troubleshooting
---------------

- **OOM while logging metrics**: Reduce ``metric_buffer_size`` or ``max_metrics_per_run``. The tracker warns when it
  evicts entries.
- **Artifacts missing**: Ensure artifact paths are absolute or reside under your working directory; ``PathValidator``
  rejects ``..`` segments.
- **Databricks auth fails**: Re-export ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` and verify the URL matches the
  Unity Catalog workspace.
- **Model registry locked**: Another process may be registering the same model. Retry after a short delay—the lock
  prevents metadata corruption but demands application-side retries when registration is parallel.

Next Steps
----------

- :doc:`getting_started` to apply the same configuration flow to non-ML workloads.
- :doc:`library_usage` to embed Tauro's MLOps helpers inside your own orchestrators.
- :doc:`guides/troubleshooting` for storage, concurrency, and configuration diagnostics.
- :doc:`tutorials/streaming` to keep models fresh with streaming ETLs.MLOps Tutorial: Experiment Tracking and Model Registry
====================================================

Tauro bundles a production-ready MLOps layer that couples your pipelines with experiment tracking, artifact
management, monitoring, and optional MLflow mirroring. This tutorial takes you step-by-step through:

1. Configuring the MLOps layer and context descriptor.
2. Defining the pipeline and node that execute your training script.
3. Writing the training logic that uses ``ExperimentTracker`` and ``ModelRegistry`` helpers.
4. Executing the pipeline (CLI or programmatically) and reviewing the artifacts.

Prerequisites
-------------

- ``pip install tauro[mlops]`` to pull in the MLOps extras.
- Optional: ``pip install mlflow`` when you want Tauro to mirror runs to an MLflow tracking server.
- ``pip install scikit-learn`` (or your preferred ML framework) for the training step.
- A storage backend (local filesystem, network share, or Databricks Unity Catalog) reachable for artifacts.

Configuring Tauro's MLOps Layer
-------------------------------

Tauro drives configuration through a descriptor (``settings_json.json`` by default) that maps each environment
to the five core configuration files (``global_settings.*``, ``pipelines.*``, ``nodes.*``, ``input.*``,
``output.*``). The CLI and ``AppConfigManager`` both load this descriptor to build the execution context. The
descriptor looks like:

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

The descriptor must live next to the ``settings_*`` files at the project root. Tauro expects the ``env_config``
section to contain at least the five keys above and supports additional overrides (``mlops_config_path``) if you
need custom layers.

In ``global_settings.*`` add an ``mlops`` block to describe where experiment data lives:

.. code-block:: yaml

   mlops:
  file protects metadata consistency but requires application-side retries for parallel registries.

Next Steps
----------

- The :doc:`/docs/getting_started` guide applies the same configuration flow to non-ML workloads.
- The :doc:`/docs/library_usage` reference shows how to embed Tauro executions within other Python services.
- :doc:`/docs/guides/troubleshooting` offers deeper diagnostics for storage, concurrency, and configuration issues.MLOps Tutorial: Experiment Tracking and Model Registry
====================================================

Tauro provides a production-grade MLOps stack that lets you complement batch pipelines with experiment tracking,
artifact management, monitoring, and optional MLflow integration. This tutorial walks through a complete workflow:

1. Configure the MLOps context and storage backend.
2. Define the pipeline and node that execute the training script.
3. Use `ExperimentTracker` and `ModelRegistry` to log metrics, artifacts, and register models.
4. Inspect runs, promote models, and monitor health checks.

Prerequisites
-------------

- Tauro with MLOps extras: ``pip install tauro[mlops]``
- Optional: ``pip install mlflow`` when you want to mirror runs in MLflow.
- ``pip install scikit-learn`` for the sample training script.
- A storage backend (local filesystem or Databricks) reachable when writing artifacts and metrics.

Configure Tauro's MLOps Layer
-----------------------------

Add the following block to ``config/base/global_settings.yaml`` or an environment-specific patch so Tauro knows
where to keep experiments, models, and logs. The CLI merges this with other `global_settings` patches at startup.

.. code-block:: yaml

   mlops:
     backend_type: local          # local filesystem (default) or databricks
     storage_path: ./mlops_data   # where experiments, runs, and artifacts live
     model_retention_days: 90
     default_experiment_name: customer_churn
     experiment_tracking:
       metric_buffer_size: 200
       auto_flush_metrics: true
     registry:
       max_versions_per_model: 100
     mlflow:
       tracking_uri: http://127.0.0.1:5000
       experiment_name: customer_churn_ml

When targeting Databricks, prefer environment variables to avoid leaking secrets:

.. code-block:: bash

   export TAURO_MLOPS_BACKEND=databricks
   export TAURO_MLOPS_CATALOG=ml_catalog
   export TAURO_MLOPS_SCHEMA=experiments

- You need to have MLflow installed: `pip install mlflow`.
- You need to have scikit-learn installed: `pip install scikit-learn`.
- You have a running MLflow tracking server. For a local test, you can start one easily:
  .. code-block:: bash

     mlflow ui

  This will start an MLflow UI on `http://127.0.0.1:5000`.

Example: Training a Customer Churn Predictor
--------------------------------------------

Let's build a pipeline that trains a classification model to predict customer churn.

**1. Configure the MLOps Integration**

Tauro needs to know where your MLflow server is. This is configured in your `global_settings.yaml`.

**`config/base/global_settings.yaml`**
.. code-block:: yaml

   mlops:
     provider: mlflow
     tracking_uri: "http://127.0.0.1:5000"
     default_experiment_name: "customer_churn"

**2. Define the Training Pipeline**

Next, define the pipeline and node that will execute the training script.

**`config/base/pipelines.yaml`**
.. code-block:: yaml

   ml_training_pipeline:
     nodes:
       - train_churn_model

**`config/base/nodes.yaml`**
.. code-block:: yaml

   train_churn_model:
     module: "pipelines.ml.train_model.train_and_register"
     inputs:
       training_data: preprocessed_training_data # Defined in input.yaml
     runner: local # This is a standard python function

**3. Create the Training Script**

This is where the core ML logic resides. The script will use Tauro's `ExperimentTracker` and `ModelRegistry` to communicate with MLflow.

**`pipelines/ml/train_model.py`**
.. code-block:: python

   import pandas as pd
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.model_selection import train_test_split
   from sklearn.metrics import accuracy_score, f1_score

   from tauro.mlops import ExperimentTracker, ModelRegistry

   def train_and_register(training_data: pd.DataFrame, tracker: ExperimentTracker, registry: ModelRegistry):
       """
       Trains a churn prediction model, logs the experiment, and registers the model.
       
       Tauro automatically injects the 'tracker' and 'registry' objects
       when they are type-hinted in the function signature.
       """
       print("Starting model training process...")

       # 1. Prepare data
       X = training_data.drop('churn', axis=1)
       y = training_data['churn']
       X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

       # 2. Start an experiment run
       # This creates a new run within the 'customer_churn' experiment in MLflow.
       with tracker.start_run(run_name="random_forest_baseline") as run:
           
           # 3. Log parameters
           params = {
               "n_estimators": 150,
               "max_depth": 12,
               "min_samples_leaf": 4,
               "random_state": 42
           }
           run.log_params(params)
           print(f"Logged parameters: {params}")

           # 4. Train the model
           model = RandomForestClassifier(**params)
           model.fit(X_train, y_train)
           
           # 5. Evaluate the model
           y_pred = model.predict(X_test)
           accuracy = accuracy_score(y_test, y_pred)
           f1 = f1_score(y_test, y_pred, average='weighted')
           
           # 6. Log metrics
           metrics = {
               "accuracy": accuracy,
               "f1_score": f1
           }
           run.log_metrics(metrics)
           print(f"Logged metrics: {metrics}")
           
           # 7. Log the trained model as an artifact
           run.log_model(model, artifact_path="churn_model")
           print("Logged model artifact.")

           # 8. Register the model if it meets our quality threshold
           if accuracy > 0.88:
               print(f"Accuracy ({accuracy:.3f}) is above threshold. Registering model...")
               model_info = registry.register_model(
                   model=model,
                   model_name="churn-predictor",
                   tags={"type": "classifier", "framework": "sklearn"}
               )
               print(f"✅ Model registered as '{model_info.name}' version {model_info.version}")
           else:
               print(f"Accuracy ({accuracy:.3f}) is below threshold. Model not registered.")

**How it Works**

- **Automatic Injection**: Notice that the `train_and_register` function doesn't create the `tracker` and `registry` objects. Tauro's executor sees the type hints (`tracker: ExperimentTracker`) and automatically initializes and injects these MLOps clients for you.
- **Context Manager**: The `with tracker.start_run(...)` block ensures that all logs (params, metrics, models) are associated with a single, organized run in MLflow.
- **Decoupled Logic**: The training code is pure Python and scikit-learn. The Tauro MLOps clients are simple wrappers that decouple your logic from the specific MLOps provider (e.g., you could swap MLflow for something else with minimal code changes).

**4. Run the Pipeline**

Now, you can run the pipeline as you would any other Tauro pipeline.

.. code-block:: bash

   tauro --env dev --pipeline ml_training_pipeline

**5. View the Results in MLflow**

1.  Go to your MLflow UI (`http://127.0.0.1:5000`).
2.  You will see a new run under the `customer_churn` experiment.
3.  Click on the run to see the parameters, metrics, and the model artifact you logged.
4.  If the accuracy was high enough, you will also find a new model named `churn-predictor` in the "Models" tab.