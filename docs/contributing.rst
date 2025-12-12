Contributing
============

Thank you for your interest in contributing to Tauro! This document provides guidelines and instructions for contributing.

Code of Conduct
---------------

By participating in this project, you agree to abide by our Code of Conduct:

- Be respectful and inclusive
- Be patient and welcoming
- Be collaborative
- Be careful in the words you choose
- When in doubt, ask for clarification

How to Contribute
-----------------

Ways to Contribute
~~~~~~~~~~~~~~~~~~

- **Report Bugs**: Submit bug reports on GitHub Issues
- **Suggest Features**: Propose new features or improvements
- **Write Documentation**: Improve or add documentation
- **Write Code**: Fix bugs or implement features
- **Review Pull Requests**: Help review code submissions
- **Answer Questions**: Help others in Discussions

Reporting Bugs
--------------

Before Submitting
~~~~~~~~~~~~~~~~~

1. Check existing issues to avoid duplicates
2. Collect relevant information:
   - Tauro version (``tauro --version``)
   - Python version
   - Operating system
   - Error messages and stack traces
   - Steps to reproduce

Bug Report Template
~~~~~~~~~~~~~~~~~~~

.. code-block:: markdown

   **Description**
   A clear description of the bug.

   **To Reproduce**
   Steps to reproduce the behavior:
   1. Run command '...'
   2. See error

   **Expected Behavior**
   What you expected to happen.

   **Actual Behavior**
   What actually happened.

   **Environment**
   - OS: [e.g., Ubuntu 22.04]
   - Python: [e.g., 3.10.5]
   - Tauro: [e.g., 0.1.3]

   **Additional Context**
   Add any other context about the problem.

Suggesting Features
-------------------

Feature Request Template
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: markdown

   **Problem Statement**
   Describe the problem this feature would solve.

   **Proposed Solution**
   Describe your proposed solution.

   **Alternatives Considered**
   Describe alternatives you've considered.

   **Use Cases**
   Provide specific use cases.

   **Additional Context**
   Add any other context or screenshots.

Development Setup
-----------------

Prerequisites
~~~~~~~~~~~~~

- Python 3.9 or newer
- Git
- pip or Poetry

Fork and Clone
~~~~~~~~~~~~~~

.. code-block:: bash

   # Fork the repository on GitHub
   # Then clone your fork
   git clone https://github.com/YOUR_USERNAME/tauro.git
   cd tauro

   # Add upstream remote
   git remote add upstream https://github.com/faustino125/tauro.git

Create Virtual Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Using venv
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows

   # Or using conda
   conda create -n tauro python=3.10
   conda activate tauro

Install Dependencies
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Install in editable mode with dev dependencies
   pip install -e ".[dev]"

   # Or with Poetry
   poetry install

Install Pre-commit Hooks
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pre-commit install

Development Workflow
--------------------

1. Create a Branch
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Sync with upstream
   git fetch upstream
   git checkout main
   git merge upstream/main

   # Create feature branch
   git checkout -b feature/amazing-feature

2. Make Changes
~~~~~~~~~~~~~~~

Follow the coding standards:

- Use Black for formatting
- Use isort for import sorting
- Follow PEP 8
- Write docstrings (Google style)
- Add type hints

Example:

.. code-block:: python

   def process_data(
       data: pd.DataFrame,
       config: Dict[str, Any]
   ) -> pd.DataFrame:
       """Process input data according to configuration.
       
       Args:
           data: Input DataFrame to process.
           config: Configuration dictionary with processing rules.
       
       Returns:
           Processed DataFrame.
       
       Raises:
           ValueError: If required columns are missing.
       
       Example:
           >>> config = {"filter_col": "amount", "min_value": 100}
           >>> result = process_data(df, config)
       """
       # Implementation
       pass

3. Write Tests
~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   from tauro import PipelineExecutor

   def test_pipeline_execution():
       """Test basic pipeline execution."""
       executor = PipelineExecutor(test_context)
       result = executor.execute("test_pipeline")
       
       assert result.success
       assert result.nodes_executed > 0

   def test_error_handling():
       """Test error handling in pipeline."""
       executor = PipelineExecutor(test_context)
       
       with pytest.raises(ValueError):
           executor.execute("invalid_pipeline")

Run Tests:

.. code-block:: bash

   # Run all tests
   pytest

   # Run with coverage
   pytest --cov=src --cov-report=html

   # Run specific test
   pytest tests/test_executor.py::test_pipeline_execution

4. Format and Lint
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Format code
   black src/ tests/
   isort src/ tests/

   # Lint code
   flake8 src/ tests/
   pylint src/

   # Type check
   mypy src/

5. Update Documentation
~~~~~~~~~~~~~~~~~~~~~~~

If your changes affect user-facing functionality:

.. code-block:: bash

   # Update docstrings
   # Update .rst files in docs/
   # Build docs locally
   cd docs
   make html
   # Open _build/html/index.html

6. Commit Changes
~~~~~~~~~~~~~~~~~

Write clear, descriptive commit messages:

.. code-block:: bash

   # Good commit messages
   git commit -m "Add support for Iceberg format"
   git commit -m "Fix memory leak in streaming pipeline"
   git commit -m "Update documentation for MLOps integration"

   # Bad commit messages (avoid these)
   git commit -m "Fix bug"
   git commit -m "Update"
   git commit -m "WIP"

Commit Message Format:

.. code-block:: text

   <type>: <subject>

   <body>

   <footer>

Types:

- ``feat``: New feature
- ``fix``: Bug fix
- ``docs``: Documentation changes
- ``style``: Code style changes
- ``refactor``: Code refactoring
- ``test``: Test changes
- ``chore``: Build/tooling changes

Example:

.. code-block:: text

   feat: Add Apache Iceberg format support

   - Implement IcebergReader class
   - Implement IcebergWriter class
   - Add configuration options for Iceberg tables
   - Add tests for Iceberg operations

   Closes #123

7. Push and Create PR
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Push to your fork
   git push origin feature/amazing-feature

   # Create Pull Request on GitHub

Pull Request Template:

.. code-block:: markdown

   **Description**
   Brief description of changes.

   **Type of Change**
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update

   **Testing**
   - [ ] All tests pass
   - [ ] Added new tests
   - [ ] Manual testing completed

   **Checklist**
   - [ ] Code follows style guidelines
   - [ ] Documentation updated
   - [ ] Tests added/updated
   - [ ] Changelog updated

Coding Standards
----------------

Python Style
~~~~~~~~~~~~

- Follow PEP 8
- Use Black with default settings
- Maximum line length: 88 characters
- Use meaningful variable names
- Prefer explicit over implicit

Documentation
~~~~~~~~~~~~~

- Use Google-style docstrings
- Include examples in docstrings
- Keep documentation up to date
- Write clear commit messages

Testing
~~~~~~~

- Write tests for all new features
- Maintain >80% code coverage
- Use descriptive test names
- Test edge cases and error conditions

Project Structure
~~~~~~~~~~~~~~~~~

.. code-block:: text

   tauro/
   ├── src/
   │   └── core/
   │       ├── cli/          # CLI implementation
   │       ├── config/       # Configuration management
   │       ├── exec/         # Execution engine
   │       ├── io/           # I/O operations
   │       ├── streaming/    # Streaming pipelines
   │       └── mlops/        # MLOps integration
   ├── tests/               # Test files
   ├── docs/                # Documentation
   ├── examples/            # Usage examples
   └── pyproject.toml       # Project configuration

Review Process
--------------

What to Expect
~~~~~~~~~~~~~~

1. **Automated Checks**: CI/CD runs tests and linting
2. **Code Review**: Maintainers review your code
3. **Discussion**: Address feedback and questions
4. **Approval**: Once approved, PR is merged

Review Criteria
~~~~~~~~~~~~~~~

- **Correctness**: Code works as intended
- **Tests**: Adequate test coverage
- **Documentation**: Changes are documented
- **Style**: Follows coding standards
- **Performance**: No significant performance regression

Release Process
---------------

Tauro uses semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

Releases are created by maintainers following this process:

1. Update version in ``pyproject.toml``
2. Update ``CHANGELOG.md``
3. Create Git tag
4. Build and publish to PyPI
5. Create GitHub release

Community
---------

- **GitHub Discussions**: General questions and ideas
- **GitHub Issues**: Bug reports and feature requests
- **Email**: faustinolopezramos@gmail.com

Recognition
-----------

Contributors are recognized in:

- ``CHANGELOG.md``
- GitHub contributors page
- Release notes

License
-------

By contributing, you agree that your contributions will be licensed under the MIT License.

Questions?
----------

If you have questions about contributing, please:

1. Check existing documentation
2. Search closed issues
3. Ask in GitHub Discussions
4. Email the maintainers

Thank you for contributing to Tauro! 🎉
