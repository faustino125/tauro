# 🚀 Enhanced Tauro CLI

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Enhanced Tauro** is a modern, secure, and feature-rich data pipeline execution system with intelligent configuration auto-discovery, comprehensive monitoring, and advanced security features.

## ✨ Features

### 🔍 **Intelligent Configuration Discovery**
- **Auto-discovery** of configuration files across directory structures
- **Smart scoring system** for best configuration matching
- **Interactive selection** when multiple configurations are found
- **Multi-format support** (JSON, YAML, DSL)

### 🛡️ **Enhanced Security**
- **Path traversal protection** with comprehensive validation
- **File extension filtering** for security compliance
- **Safe directory operations** with automatic cleanup
- **Secure Python path management**

### ⚡ **Performance & Reliability**
- **Asynchronous operations** for improved performance
- **Thread-safe caching** with automatic expiration
- **Concurrent configuration discovery** using thread pools
- **Comprehensive error handling** with detailed diagnostics

### 📊 **Advanced Monitoring**
- **Execution metrics** with timing and success tracking
- **Structured logging** with multiple output formats
- **Real-time progress** with emoji-enhanced output
- **Detailed error diagnostics** with suggestions

### 🔧 **Developer Experience**
- **Interactive mode** for configuration selection
- **Dry-run capability** for safe testing
- **Validation-only mode** for configuration checking
- **Comprehensive help** with usage examples

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd enhanced-tauro

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Basic Usage

```bash
# Execute a pipeline in development environment
tauro --env dev --pipeline data_processing

# Execute with specific node
tauro --env prod --pipeline etl --node transform_data

# Interactive configuration selection
tauro --env dev --pipeline test --interactive

# List available configurations
tauro --list-configs
```

## 📖 Documentation

### Command Line Options

#### **Core Execution**
```bash
--env {base,dev,pre_prod,prod}  # Execution environment (required)
--pipeline PIPELINE             # Pipeline name to execute (required)
--node NODE                     # Specific node to execute (optional)
--start-date YYYY-MM-DD         # Processing start date
--end-date YYYY-MM-DD           # Processing end date
```

#### **Configuration Discovery**
```bash
--base-path PATH                # Base path for configuration discovery
--layer-name LAYER              # Target layer name for matching
--use-case USE_CASE             # Target use case for matching
--config-type {yml,json,dsl}    # Preferred configuration type
--interactive                   # Enable interactive selection
--list-configs                  # List all discovered configurations
```

#### **Logging & Output**
```bash
--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}  # Logging level
--log-file FILE                 # Custom log file path
--verbose, -v                   # Enable verbose output (DEBUG level)
--quiet, -q                     # Suppress output except errors
```

#### **Operation Modes**
```bash
--validate-only                 # Validate configuration without execution
--dry-run                       # Show execution plan without running
--version                       # Show version information
```

### Configuration Formats

#### **Unified JSON Configuration**
```json
{
  "base_path": "/path/to/project",
  "global_settings": {
    "project_name": "my_project",
    "version": "1.0.0"
  },
  "pipeline": {
    "data_processing": {
      "description": "Main data processing pipeline"
    }
  },
  "node": {
    "extract": {
      "type": "extractor",
      "config": {}
    }
  },
  "input": {
    "source_data": {
      "type": "csv",
      "path": "/data/input.csv"
    }
  },
  "output": {
    "processed_data": {
      "type": "parquet",
      "path": "/data/output.parquet"
    }
  }
}
```

#### **Environment-based Configuration**
```json
{
  "base_path": "/path/to/project",
  "env_config": {
    "base": {
      "global_settings_path": "config/global_settings.yml",
      "pipelines_config_path": "config/pipelines.yml"
    },
    "dev": {
      "nodes_config_path": "config/dev/nodes.yml",
      "input_config_path": "config/dev/input.yml",
      "output_config_path": "config/dev/output.yml"
    },
    "prod": {
      "nodes_config_path": "config/prod/nodes.yml",
      "input_config_path": "config/prod/input.yml",
      "output_config_path": "config/prod/output.yml"
    }
  }
}
```

## 🎯 Usage Examples

### **Basic Pipeline Execution**
```bash
# Execute a simple pipeline
tauro --env dev --pipeline data_processing

# Execute with date range
tauro --env prod --pipeline etl \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### **Configuration Discovery**
```bash
# Auto-discover configuration in specific layer
tauro --env dev --pipeline clustering \
  --layer-name golden_layer

# Target specific use case
tauro --env dev --pipeline analysis \
  --use-case customer_segmentation

# Prefer specific configuration type
tauro --env dev --pipeline test \
  --config-type yml \
  --interactive
```

### **Validation and Testing**
```bash
# Validate configuration without execution
tauro --env dev --pipeline test --validate-only

# Dry run to see execution plan
tauro --env prod --pipeline etl --dry-run

# Verbose output for debugging
tauro --env dev --pipeline debug \
  --verbose \
  --log-file debug.log
```

### **Configuration Management**
```bash
# List all available configurations
tauro --list-configs

# List configurations in specific path
tauro --list-configs --base-path /custom/path

# Interactive selection with verbose output
tauro --env dev --pipeline demo \
  --interactive \
  --verbose
```

## 🏗️ Architecture

### **Core Components**

```
Enhanced Tauro CLI
├── SecurityManager          # Path validation and security
├── CacheManager             # Thread-safe caching system
├── LoggerConfigurator       # Advanced logging setup
├── ConfigurationDiscovery   # Intelligent config discovery
├── ConfigurationManager     # Unified config management
├── PythonPathManager        # Safe path manipulation
├── PipelineExecutorWrapper  # Enhanced pipeline execution
├── ContextInitializer       # Context setup and validation
├── ConfigurationValidator   # Comprehensive validation
└── TauroCLI                 # Main CLI interface
```

### **Configuration Loaders**
- **JSONConfigLoader**: Enhanced JSON parsing with validation
- **YAMLConfigLoader**: YAML support with PyYAML integration
- **DSLConfigLoader**: Custom DSL format with type inference

### **Security Features**
- Path traversal protection
- File extension validation
- Safe directory operations
- Secure Python path management

## 🔧 Development

### **Project Structure**
```
enhanced-tauro/
├── tauro.py                 # Main CLI implementation
├── requirements.txt         # Python dependencies
├── setup.py                # Package setup
├── tests/                  # Test suite
│   ├── test_security.py
│   ├── test_discovery.py
│   └── test_execution.py
├── examples/               # Usage examples
│   ├── configs/
│   └── pipelines/
└── docs/                   # Documentation
    ├── configuration.md
    └── troubleshooting.md
```

### **Dependencies**
```txt
loguru>=0.7.0              # Advanced logging
PyYAML>=6.0                # YAML support (optional)
```

### **Testing**
```bash
# Run tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=tauro

# Run specific test category
python -m pytest tests/test_security.py -v
```

## 🚨 Troubleshooting

### **Common Issues**

#### **Import Errors**
```bash
# Enable verbose mode for detailed diagnostics
tauro --env dev --pipeline test --verbose

# Check Python path configuration
tauro --env dev --pipeline test --dry-run --verbose
```

#### **Configuration Not Found**
```bash
# List available configurations
tauro --list-configs

# Use interactive mode to select manually
tauro --env dev --pipeline test --interactive

# Specify custom base path
tauro --env dev --pipeline test --base-path /custom/path
```

#### **Permission Errors**
```bash
# Check file permissions
ls -la config/

# Ensure directories are accessible
tauro --validate-only --verbose
```

### **Debugging Tips**

1. **Use `--verbose` flag** for detailed output
2. **Check logs** in `logs/tauro.log`
3. **Use `--dry-run`** to test without execution
4. **Enable structured logging** for analysis
5. **Use `--validate-only`** to check configuration

## 📝 Migration Guide

### **From Original Tauro**

The enhanced version maintains **100% backward compatibility** with original Tauro configurations. Simply replace the old script with the new one:

```bash
# Old usage (still works)
python tauro_cli.py --env dev --pipeline test

# New enhanced usage (recommended)
tauro --env dev --pipeline test

# New features available
tauro --env dev --pipeline test --interactive --verbose
```

### **Configuration Migration**

No configuration changes required. The enhanced version supports:
- ✅ All original configuration formats
- ✅ Existing directory structures
- ✅ Current pipeline definitions
- ✅ Existing environment setups

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### **Development Guidelines**
- Follow PEP 8 style guidelines
- Add tests for new features
- Update documentation
- Use type hints
- Write descriptive commit messages

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Original Tauro CLI developers
- Python logging community
- Contributors and testers

## 📞 Support

- 📧 **Email**: support@example.com
- 🐛 **Issues**: [GitHub Issues](https://github.com/your-repo/enhanced-tauro/issues)
- 📖 **Documentation**: [Full Documentation](https://your-docs.com)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/your-repo/enhanced-tauro/discussions)

---

**Made with ❤️ for the data engineering community**
