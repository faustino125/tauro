API Reference
=============

This reference documents the public API of Tauro. These classes and functions are available directly from the ``tauro`` package.

Configuration
-------------

.. autoclass:: core.Context
   :members:
   :undoc-members:

.. autoclass:: core.ContextLoader
   :members:
   :undoc-members:

.. autoclass:: core.ConfigLoader
   :members:
   :undoc-members:

.. autoclass:: core.ContextFactory
   :members:
   :undoc-members:

Execution
---------

.. autoclass:: core.PipelineExecutor
   :members:
   :undoc-members:

.. autoclass:: core.BatchExecutor
   :members:
   :undoc-members:

.. autoclass:: core.StreamingExecutor
   :members:
   :undoc-members:

.. autoclass:: core.HybridExecutor
   :members:
   :undoc-members:

.. autoclass:: core.RetryPolicy
   :members:
   :undoc-members:

IO & Data Management
--------------------

.. autoclass:: core.InputLoader
   :members:
   :undoc-members:

.. autoclass:: core.ReaderFactory
   :members:
   :undoc-members:

.. autoclass:: core.WriterFactory
   :members:
   :undoc-members:

.. autoclass:: core.DataOutputManager
   :members:
   :undoc-members:

Streaming
---------

.. autoclass:: core.StreamingPipelineManager
   :members:
   :undoc-members:

.. autoclass:: core.StreamingQueryManager
   :members:
   :undoc-members:

MLOps
-----

.. autoclass:: core.MLOpsContext
   :members:
   :undoc-members:

.. autoclass:: core.MLOpsConfig
   :members:
   :undoc-members:

.. autoclass:: core.ModelRegistry
   :members:
   :undoc-members:

.. autoclass:: core.ExperimentTracker
   :members:
   :undoc-members:
