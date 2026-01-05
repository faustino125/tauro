Getting Started
===============

This guide will have you running your first Tauro pipeline in 10 minutes.

Prerequisites
-------------

- **Python 3.10 or higher** (check with ``python --version``)
- **pip** (included with Python)

Installation
~~~~~~~~~~~~

**Step 1: Set up a virtual environment (recommended)**

.. code-block:: bash

   # On Mac/Linux
   python3 -m venv tauro-env
   source tauro-env/bin/activate

   # On Windows
   python -m venv tauro-env
   tauro-env\Scripts\activate

**Step 2: Install Tauro**

.. code-block:: bash

   pip install tauro

Create Your First Project
-------------------------

Tauro includes templates that set up everything you need. Create a project:

.. code-block:: bash

   tauro template --template medallion_basic --project-name my_project
   cd my_project

Your project structure:

.. code-block:: text

   my_project/
   ├── config/
   │   ├── pipelines.yaml      # Pipeline definitions
   │   ├── nodes.yaml          # Node configurations
   │   ├── inputs.yaml         # Data sources
   │   ├── outputs.yaml        # Output targets
   │   └── global.yaml         # General settings
   ├── src/
   │   └── nodes/              # Pipeline code
   │       ├── extract.py
   │       ├── transform.py
   │       └── load.py
   ├── data/
   │   ├── input/              # Input data
   │   └── output/             # Results
   └── settings_json.json      # Environment descriptor

Run Your First Pipeline
------------------------

.. code-block:: bash

   tauro run --env dev --pipeline sample_pipeline

You should see output like:

.. code-block:: text

   ✓ Loading configuration...
   ✓ Validating pipeline...
   ✓ Executing nodes...
   ✓ extract ..................... [1/3]
   ✓ transform ................... [2/3]
   ✓ load ....................... [3/3]
   ✓ Pipeline completed in 2.3 seconds

Congratulations! Your pipeline is running.

How It Works
~~~~~~~~~~~~

Tauro executed three steps defined in your configuration:

1. **extract** - Read data from ``data/input/``
2. **transform** - Process and clean the data
3. **load** - Save results to ``data/output/``

Each step is defined in ``config/nodes.yaml`` and the code lives in ``src/nodes/``.

Understand the Configuration
-----------------------------

Open ``config/pipelines.yaml``:

.. code-block:: yaml

   pipelines:
     sample_pipeline:
       nodes: [extract, transform, load]
       description: "A simple ETL pipeline"

This defines a pipeline named `sample_pipeline` with three steps executed in order.

Open ``config/nodes.yaml``:

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract.extract_data"
       description: "Read data from CSV"
       timeout: 300

     transform:
       function: "src.nodes.transform.clean_data"
       description: "Clean and process data"
       timeout: 600

     load:
       function: "src.nodes.load.save_results"
       description: "Save processed data"
       timeout: 300

Each node points to a Python function. Here's an example from ``src/nodes/extract.py``:

.. code-block:: python

   import pandas as pd

   def extract_data():
       """Read data from CSV file."""
       df = pd.read_csv("data/input/sample.csv")
       return df

Tauro passes the result to the next node automatically.

Next Steps
----------

Explore these guides to continue learning:

- **:doc:`cli_usage`** - Learn all CLI commands
- **:doc:`configuration`** - Master pipeline configuration
- **:doc:`best_practices`** - Follow best practices
- **:doc:`tutorials/batch_etl`** - Build a real ETL pipeline
- **:doc:`advanced/troubleshooting`** - Solve common problems

For complete API reference, see :doc:`api/reference`.
