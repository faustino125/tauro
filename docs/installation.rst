Installation
=============

System Requirements
-------------------

- **Python**: 3.10 or newer
- **Operating System**: Linux, macOS, or Windows
- **Memory**: 2GB RAM minimum (8GB+ recommended for Spark)
- **Disk Space**: 500MB minimum

Quick Installation
------------------

**Step 1: Create a virtual environment (recommended)**

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

For large-scale data processing with Spark:

.. code-block:: bash

   pip install tauro[spark]

Verify Installation
-------------------

Check your installation:

.. code-block:: bash

   tauro --version

Expected output:

.. code-block:: text

   Tauro version 0.1.3

Optional Features
-----------------

**With MLOps Support**

.. code-block:: bash

   pip install tauro[mlops]

**With All Features**

.. code-block:: bash

   pip install tauro[all]

This includes Spark, MLOps, and other integrations.

Platform-Specific Notes
-----------------------

**Linux (Ubuntu/Debian)**

.. code-block:: bash

   sudo apt-get update
   sudo apt-get install python3-pip python3-venv
   python3 -m venv tauro-env
   source tauro-env/bin/activate
   pip install tauro

**macOS**

.. code-block:: bash

   python3 -m venv tauro-env
   source tauro-env/bin/activate
   pip install tauro

**Windows (PowerShell)**

.. code-block:: powershell

   python -m venv tauro-env
   .\tauro-env\Scripts\Activate.ps1
   pip install tauro

Troubleshooting
---------------

**"tauro: command not found"**

Make sure your virtual environment is activated or run:

.. code-block:: bash

   python -m tauro --version

**Permission errors**

Use user installation:

.. code-block:: bash

   pip install --user tauro

**Dependency conflicts**

Create a fresh environment:

.. code-block:: bash

   python -m venv tauro-env
   source tauro-env/bin/activate
   pip install --upgrade pip
   pip install tauro

Next Steps
----------

- Start with :doc:`getting_started`
- Explore :doc:`cli_usage` for CLI commands
- See :doc:`configuration` for setup options
