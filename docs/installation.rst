Installation
============

This guide covers all installation methods for Tauro.

Requirements
------------

System Requirements
~~~~~~~~~~~~~~~~~~~

- **Python**: 3.9 or newer (3.10+ recommended)
- **Operating System**: Linux, macOS, or Windows
- **Memory**: Minimum 2GB RAM (8GB+ recommended for Spark)
- **Disk Space**: 500MB minimum

Optional Requirements
~~~~~~~~~~~~~~~~~~~~~

For Spark Support
^^^^^^^^^^^^^^^^^

- Apache Spark 3.4 or newer
- Java 8 or 11 (required by Spark)
- Hadoop (for HDFS support)

For Streaming
^^^^^^^^^^^^^

- Kafka 2.8+ (for Kafka streaming)
- AWS credentials (for Kinesis streaming)

For MLOps
^^^^^^^^^

- MLflow 2.0+ (included in extras)

Installation Methods
--------------------

Method 1: pip (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Basic Installation
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pip install tauro

This installs the core framework with:

- CLI interface
- Batch processing
- Configuration management
- I/O operations

With Spark Support
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pip install tauro[spark]

Includes:

- Apache Spark integration
- Large-scale data processing
- Distributed computing support

With API and Monitoring
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pip install tauro[api,monitoring]

Includes:

- FastAPI support
- Prometheus metrics
- Health checks
- REST API interface

Complete Installation
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pip install tauro[all]

Includes everything:

- Spark support
- API and monitoring
- MLOps integration
- Streaming support
- All optional dependencies

Method 2: Poetry
~~~~~~~~~~~~~~~~

If you use Poetry for dependency management:

.. code-block:: bash

   poetry add tauro

With extras:

.. code-block:: bash

   poetry add tauro[all]

Method 3: From Source
~~~~~~~~~~~~~~~~~~~~~

For development or latest features:

.. code-block:: bash

   # Clone repository
   git clone https://github.com/faustino125/tauro.git
   cd tauro

   # Install in editable mode
   pip install -e .

   # Or with Poetry
   poetry install

Method 4: Docker
~~~~~~~~~~~~~~~~

Using Docker (recommended for production):

.. code-block:: bash

   # Pull image
   docker pull faustino125/tauro:latest

   # Run container
   docker run -it faustino125/tauro:latest tauro --help

With docker-compose:

.. code-block:: yaml

   version: '3.8'
   services:
     tauro:
       image: faustino125/tauro:latest
       volumes:
         - ./config:/app/config
         - ./data:/app/data
       command: tauro --env prod --pipeline etl

Verify Installation
-------------------

Check Version
~~~~~~~~~~~~~

.. code-block:: bash

   tauro --version

Expected output:

.. code-block:: text

   Tauro version 0.1.3

Check Installation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   python -c "import tauro; print(tauro.__version__)"

List Available Commands
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --help

Upgrading
---------

Upgrade to Latest Version
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pip install --upgrade tauro

Upgrade with Extras
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pip install --upgrade tauro[all]

Upgrade from Source
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   cd tauro
   git pull
   pip install -e . --upgrade

Uninstallation
--------------

Using pip
~~~~~~~~~

.. code-block:: bash

   pip uninstall tauro

Using Poetry
~~~~~~~~~~~~

.. code-block:: bash

   poetry remove tauro

Troubleshooting
---------------

Installation Fails with Permission Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use user installation:

.. code-block:: bash

   pip install --user tauro

Or use virtual environment:

.. code-block:: bash

   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   pip install tauro

Dependency Conflicts
~~~~~~~~~~~~~~~~~~~~

Create a fresh environment:

.. code-block:: bash

   conda create -n tauro python=3.10
   conda activate tauro
   pip install tauro[all]

Spark Installation Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~

Install Java first:

.. code-block:: bash

   # Ubuntu/Debian
   sudo apt-get install openjdk-11-jdk

   # macOS
   brew install openjdk@11

   # Windows
   # Download from https://adoptium.net/

Then install Spark:

.. code-block:: bash

   pip install pyspark==3.4.0

SSL Certificate Errors
~~~~~~~~~~~~~~~~~~~~~~~

Use trusted host:

.. code-block:: bash

   pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org tauro

Slow Installation
~~~~~~~~~~~~~~~~~

Use a mirror:

.. code-block:: bash

   pip install -i https://pypi.tuna.tsinghua.edu.cn/simple tauro

Development Installation
------------------------

For Contributors
~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Clone repository
   git clone https://github.com/faustino125/tauro.git
   cd tauro

   # Create virtual environment
   python -m venv venv
   source venv/bin/activate

   # Install with dev dependencies
   pip install -e ".[dev]"

   # Install pre-commit hooks
   pre-commit install

   # Run tests
   pytest

Platform-Specific Notes
-----------------------

Linux
~~~~~

.. code-block:: bash

   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install python3-pip python3-venv
   pip3 install tauro

macOS
~~~~~

.. code-block:: bash

   # Using Homebrew
   brew install python@3.10
   pip3 install tauro

Windows
~~~~~~~

.. code-block:: powershell

   # Using PowerShell
   python -m pip install --upgrade pip
   pip install tauro

Next Steps
----------

After installation:

1. Follow :doc:`getting_started` for your first pipeline
2. Explore :doc:`cli_usage` for CLI commands
3. Read :doc:`library_usage` for programmatic usage
4. Check :doc:`configuration` for setup options
