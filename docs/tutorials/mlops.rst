MLOps Tutorial: Experiment Tracking and Model Registry
==================================================

Tauro includes features to help you operationalize your machine learning workflows, integrating seamlessly with tools like MLflow for experiment tracking and model registration.

This tutorial will guide you through a typical ML workflow using Tauro's MLOps capabilities:
1.  Train a machine learning model.
2.  Log the model's parameters, metrics, and artifacts using Tauro's experiment tracker.
3.  Register the trained model in the model registry if it meets performance criteria.

Prerequisites
-------------

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