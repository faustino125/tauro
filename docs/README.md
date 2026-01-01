# Tauro Documentation

Welcome to the Tauro documentation. This directory contains all documentation built with Sphinx and hosted on Read the Docs.

## Getting Started

For users, documentation is available at:
- **Online**: https://tauro.readthedocs.io
- **Offline**: Build locally (see below)

**First time with Tauro?** Start with [Getting Started](getting_started.rst)

## For Documentation Developers

### Building Locally

**Prerequisites**

```bash
# Install dependencies
pip install -r requirements.txt
```

**Build HTML**

```bash
# On Linux/Mac
make html

# On Windows
make.bat html

# View output
open _build/html/index.html  # Mac
xdg-open _build/html/index.html  # Linux
start _build/html/index.html  # Windows
```

**Build Other Formats**

```bash
make latexpdf  # PDF (requires LaTeX)
make epub      # EPUB ebook
make text      # Plain text
make clean     # Clean build files
```

### Documentation Structure

```
docs/
├── index.rst                  # Main documentation home
├── getting_started.rst        # Quick start guide
├── quick_reference.rst        # Command cheat sheet
├── installation.rst           # Installation instructions
├── cli_usage.rst             # Command-line interface
├── library_usage.rst         # Python library usage
├── configuration.rst         # Configuration guide
├── best_practices.rst        # Best practices
├── migration_guide.rst       # CLI to library migration
├── glossary.rst              # Terminology
├── databricks_setup.rst      # Databricks integration
├── feature_store.rst         # Feature store guide
│
├── tutorials/                 # Step-by-step examples
│   ├── batch_etl.rst
│   ├── streaming.rst
│   ├── mlops.rst
│   ├── airflow_integration.rst
│   └── fastapi_integration.rst
│
├── advanced/                  # Advanced topics
│   ├── architecture.rst
│   ├── security.rst
│   ├── performance.rst
│   ├── testing.rst
│   └── troubleshooting.rst
│
├── api/                       # API reference
│   ├── index.rst
│   └── reference.rst
│
├── changelog.rst              # Version history
├── contributing.rst           # How to contribute
├── license.rst                # MIT license
│
├── conf.py                    # Sphinx configuration
├── Makefile                   # Build automation (Mac/Linux)
├── make.bat                   # Build automation (Windows)
└── requirements.txt           # Dependencies
```

### Documentation Standards

**Format**: All documentation uses reStructuredText (.rst)

**Style Guide**:
- Use clear, professional English
- Write for end users, not implementation details
- Include practical examples
- Use relative paths for links: `:doc:\`getting_started\``
- Add table of contents to long documents

**Testing**:
- Run `make html` before committing
- Check for broken links in Sphinx output
- Test code examples work

### Adding New Documentation

1. Create `.rst` file in appropriate directory
2. Add entry to `index.rst` toctree
3. Follow style guide above
4. Test with `make html`
5. Commit and push

### Common Tasks

**Add new tutorial**
```bash
# Create file
touch tutorials/my_tutorial.rst

# Add to index.rst tutorials section
# Edit index.rst and add line to tutorials toctree
```

**Fix typos or improve existing docs**
```bash
# Edit the .rst file directly
# Test with make html
# Commit changes
```

**Add API documentation**
```bash
# Add content to api/reference.rst
# Update api/index.rst if needed
# Test with make html
```

## Sphinx Configuration

Configuration is in `conf.py`. Key settings:
- **Theme**: Read the Docs theme
- **Extensions**: sphinx.ext.autodoc, sphinx.ext.viewcode, etc.
- **HTML output**: `_build/html/`

## Read the Docs Integration

Documentation is automatically built and published when changes are pushed to main branch.

Configuration: `.readthedocs.yaml` (in project root)

## Questions?

For documentation improvements or issues, see [CONTRIBUTING.md](../CONTRIBUTING.md)


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
