Testing Your Pipelines
======================

Testing is a critical part of building reliable data pipelines. Tauro is designed to be highly testable, allowing you to write automated tests for your configuration, data transformations, and overall pipeline structure.

This guide demonstrates how to use the `pytest` framework to test a Tauro project.

**Why Test Your Pipelines?**

- **Catch Errors Early**: Find bugs in your transformation logic or configuration before they reach production.
- **Prevent Regressions**: Ensure that new changes don't break existing functionality.
- **Improve Code Quality**: Writing tests forces you to create modular and reusable nodes.
- **Documentation**: Tests serve as living documentation for how your pipelines are intended to work.

Setting Up a Test Environment
-----------------------------

A best practice is to create a dedicated `test` environment in your Tauro project. This allows you to use smaller, sample datasets and different configurations (e.g., writing to a temporary directory instead of a production bucket).

1.  In your `config` directory, create a new folder named `test`.
2.  Inside `config/test`, you can create files like `input.yaml` or `output.yaml` to override your base configuration for testing purposes.

For example, your `config/test/output.yaml` could point all outputs to a temporary directory:

.. code-block:: yaml

   # config/test/output.yaml
   default_path: "./data/test_output"

Your test environment will inherit all settings from `base` and override them with any settings you define in `test`.

Writing Tests with Pytest
-------------------------

Here's how to structure your tests using `pytest` fixtures to efficiently load the Tauro context and executor.

**1. Create Test Fixtures**

In your test file (e.g., `tests/test_pipelines.py`), first define fixtures to provide a shared `Context` and `PipelineExecutor` for your tests. This avoids reloading the configuration for every single test function.

.. code-block:: python
   # tests/test_pipelines.py

   import pytest
   from tauro import PipelineExecutor, ContextLoader, ExecutionResult
   import os

   @pytest.fixture(scope="module")
   def project_path() -> str:
       """Returns the absolute path to the Tauro project root."""
       # Assumes your tests are run from the project root.
       # Adjust if your test directory is located elsewhere.
       return os.getcwd()

   @pytest.fixture(scope="module")
   def test_context(project_path: str):
       """
       Loads the 'test' environment context once for all tests in the module.
       """
       return ContextLoader(project_path).load_from_env("test")

   @pytest.fixture(scope="module")
   def executor(test_context):
       """Provides a PipelineExecutor initialized with the test context."""
       return PipelineExecutor(test_context)

**2. Write Pipeline Tests**

Use the `executor` fixture to run tests against your full pipelines.

.. code-block:: python

   def test_load_pipeline_success(executor: PipelineExecutor):
       """Tests the successful execution of the 'load' pipeline."""
       result = executor.execute("load")
       
       assert result.success, f"Pipeline failed with: {result.error_message}"
       assert "load_raw_data" in result.nodes_executed

   def test_transform_pipeline_validation(executor: PipelineExecutor):
       """Tests that the 'transform' pipeline is valid."""
       is_valid, message = executor.validate("transform")
       
       assert is_valid, f"Validation failed: {message}"

**3. Write Node-Level Tests**

You can also test individual nodes, which is faster and more focused.

.. code-block:: python

   def test_single_node_execution(executor: PipelineExecutor):
       """Tests the execution of a single node from the 'transform' pipeline."""
       # Assuming you have a node named 'clean_and_enrich_data' in your transform pipeline
       result = executor.execute_node("transform", "clean_and_enrich_data")
       
       assert result.success
       assert len(result.nodes_executed) == 1
       assert result.nodes_executed[0] == "clean_and_enrich_data"

**4. Test for Expected Failures**

It's also important to test that your pipeline fails when it should.

.. code-block:: python

   def test_nonexistent_pipeline_fails(executor: PipelineExecutor):
       """Tests that executing a pipeline that doesn't exist raises an error."""
       with pytest.raises(ValueError, match="not found in configuration"):
           executor.execute("pipeline_that_does_not_exist")

Running Your Tests
------------------

From your project's root directory, simply run `pytest`:

.. code-block:: bash

   pytest tests/

Pytest will automatically discover and run your tests, fixtures, and assertions.