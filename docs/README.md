# Tauro Documentation

This directory contains the documentation for Tauro, built with Sphinx and hosted on Read the Docs.

## Building Documentation Locally

### Prerequisites

```bash
# Install documentation dependencies
pip install -r requirements.txt
```

### Build HTML Documentation

```bash
# On Linux/Mac
make html

# On Windows
make.bat html

# View documentation
# Open _build/html/index.html in your browser
```

### Build Other Formats

```bash
# PDF (requires LaTeX)
make latexpdf

# EPUB
make epub

# Plain text
make text
```

### Clean Build Files

```bash
make clean
```

## Documentation Structure

```
docs/
├── conf.py                 # Sphinx configuration
├── index.rst              # Documentation home page
├── getting_started.rst    # Getting started guide
├── installation.rst       # Installation instructions
├── cli_usage.rst          # CLI usage guide
├── library_usage.rst      # Library usage guide
├── configuration.rst      # Configuration guide
├── best_practices.rst     # Best practices
├── changelog.rst          # Version history
├── contributing.rst       # Contribution guidelines
├── license.rst            # License information
├── migration_guide.md     # CLI to Library migration
├── api/                   # API reference
│   ├── index.rst
│   ├── core.rst
│   ├── cli.rst
│   ├── config.rst
│   ├── exec.rst
│   ├── io.rst
│   ├── streaming.rst
│   └── mlops.rst
├── tutorials/             # Tutorials
│   ├── index.rst
│   ├── batch_etl.rst
│   ├── streaming.rst
│   ├── mlops.rst
│   ├── airflow_integration.rst
│   └── fastapi_integration.rst
├── advanced/              # Advanced topics
│   ├── architecture.rst
│   ├── security.rst
│   ├── performance.rst
│   ├── testing.rst
│   └── troubleshooting.rst
├── _static/               # Static files (CSS, images)
│   └── custom.css
├── _templates/            # Custom templates
├── requirements.txt       # Documentation dependencies
├── Makefile              # Build automation (Linux/Mac)
└── make.bat              # Build automation (Windows)
```

## Read the Docs Configuration

The documentation is automatically built and published on Read the Docs when changes are pushed to the main branch.

Configuration file: `.readthedocs.yaml` (in project root)

## Writing Documentation

### Style Guide

- Use reStructuredText (.rst) for documentation files
- Markdown (.md) is supported via MyST parser
- Follow the existing structure and formatting
- Include code examples for all features
- Add cross-references using `:doc:` and `:ref:` roles

### Code Examples

Use appropriate highlighting:

```rst
.. code-block:: python

   from tauro import PipelineExecutor
   executor = PipelineExecutor(context)
```

```rst
.. code-block:: bash

   tauro --env dev --pipeline sales
```

### Cross-References

Link to other documentation:

```rst
See :doc:`getting_started` for installation.
See :ref:`configuration` for config details.
See :class:`PipelineExecutor` for API docs.
```

### Admonitions

Use admonitions for important information:

```rst
.. note::
   This is a note.

.. warning::
   This is a warning.

.. tip::
   This is a tip.

.. danger::
   This is important!
```

## Contributing Documentation

1. Make your changes in the appropriate `.rst` files
2. Build documentation locally to verify
3. Commit and push your changes
4. Documentation will be automatically rebuilt on Read the Docs

## Documentation Standards

- All public APIs must be documented with docstrings
- All CLI commands must be documented
- All configuration options must be documented
- Include practical examples
- Keep documentation up to date with code changes

## Auto-generating API Documentation

To regenerate API documentation from docstrings:

```bash
sphinx-apidoc -f -o docs/api src/core/
```

## Troubleshooting

### Build Errors

If you encounter build errors:

```bash
# Clean and rebuild
make clean
make html
```

### Missing Modules

If autodoc can't find modules:

```bash
# Ensure Tauro is installed
pip install -e .

# Check Python path in conf.py
sys.path.insert(0, os.path.abspath('../src'))
```

### Theme Issues

If the theme doesn't look right:

```bash
# Reinstall theme
pip install --upgrade sphinx-rtd-theme
```

## Live Preview

For live preview during development:

```bash
# Install sphinx-autobuild
pip install sphinx-autobuild

# Run live server
sphinx-autobuild docs docs/_build/html

# Open http://localhost:8000
```

## Resources

- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [Read the Docs Guide](https://docs.readthedocs.io/)
- [reStructuredText Primer](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)
- [MyST Markdown](https://myst-parser.readthedocs.io/)
