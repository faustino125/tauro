"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Environment and configuration validation for Tauro projects.
Validates consistency and completeness of environment configurations.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional
import json

from loguru import logger
from engine.cli.core import get_fallback_chain


class ValidationSeverity(str, Enum):
    """Severity levels for validation errors"""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationError:
    """Represents a single validation error or warning"""

    severity: ValidationSeverity
    message: str
    environment: Optional[str] = None
    path: Optional[Path] = None
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        base = f"[{self.severity.upper()}] {self.message}"
        if self.path:
            base += f" ({self.path})"
        if self.suggestion:
            base += f"\nSuggestion: {self.suggestion}"
        return base


class EnvironmentConfigValidator:
    """
    Validates completeness and consistency of environment configurations.

    Checks:
    - Base environment exists
    - All declared environments have directories
    - Fallback chains are valid
    - Required config sections exist
    - Config files are valid (JSON/YAML syntax)
    - No missing required fields
    """

    REQUIRED_CONFIG_SECTIONS = [
        "global_settings",
        "pipelines",
        "nodes",
        "input",
        "output",
    ]

    SUPPORTED_EXTENSIONS = [".yaml", ".yml", ".json", ".dsl"]

    def __init__(self, project_path: Path):
        """
        Initialize validator for a project.

        Args:
            project_path: Root path of the Tauro project
        """
        self.project_path = Path(project_path)
        self.config_dir = self.project_path / "config"
        self.settings_file = self._find_settings_file()
        self.errors: List[ValidationError] = []

    def validate_project(self) -> List[ValidationError]:
        """
        Validate entire project configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        self.errors = []

        logger.debug(f"Starting project validation for: {self.project_path}")

        # 1. Check base configuration exists
        self._validate_base_exists()

        # 2. Check settings file
        if self.settings_file:
            self._validate_settings_file()
        else:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.WARNING,
                    message="No settings file found (settings.json, settings_yml.json, etc.)",
                    path=self.config_dir,
                    suggestion="Create one of: settings.json, settings_yml.json, or settings_dsl.json",
                )
            )

        # 3. Validate each environment
        self._validate_environments()

        # 4. Validate fallback chains
        self._validate_fallback_chains()

        # 5. Check config file syntax
        self._validate_config_syntax()

        logger.debug(f"Validation complete. Found {len(self.errors)} issues.")

        return self.errors

    def _find_settings_file(self) -> Optional[Path]:
        """Find settings file in priority order"""
        candidates = [
            self.project_path / "settings_yml.json",
            self.project_path / "settings_json.json",
            self.project_path / "settings_dsl.json",
            self.project_path / "settings.json",
            self.project_path / "config.json",
            self.project_path / "tauro.json",
        ]

        for candidate in candidates:
            if candidate.exists():
                logger.debug(f"Found settings file: {candidate}")
                return candidate

        return None

    def _validate_base_exists(self) -> None:
        """Check that base configuration directory/files exist"""
        base_dir = self.config_dir / "base"

        # Base can be directly in config/ or in config/base/
        has_base_files = False

        # Check for files in config/ (base level)
        for section in self.REQUIRED_CONFIG_SECTIONS:
            for ext in self.SUPPORTED_EXTENSIONS:
                if (self.config_dir / f"{section}{ext}").exists():
                    has_base_files = True
                    break
            if has_base_files:
                break

        # Check for files in config/base/
        if base_dir.exists():
            has_base_files = any(
                (base_dir / f"{section}{ext}").exists()
                for section in self.REQUIRED_CONFIG_SECTIONS
                for ext in self.SUPPORTED_EXTENSIONS
            )

        if not has_base_files:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message="Base configuration not found",
                    path=self.config_dir,
                    suggestion="Create config/global_settings.yaml (or .json/.dsl) with base configuration",
                )
            )

    def _validate_settings_file(self) -> None:
        """Validate settings file structure"""
        if not self.settings_file or not self.settings_file.exists():
            return

        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                settings = json.load(f)
        except json.JSONDecodeError as e:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message=f"Invalid JSON in settings file: {e}",
                    path=self.settings_file,
                    suggestion="Fix JSON syntax in settings file",
                )
            )
            return
        except Exception as e:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message=f"Error reading settings file: {e}",
                    path=self.settings_file,
                )
            )
            return

        # Check for env_config section
        if "env_config" not in settings:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message="Missing 'env_config' section in settings file",
                    path=self.settings_file,
                    suggestion="Add 'env_config' dict with environment mappings",
                )
            )
            return

        env_config = settings.get("env_config", {})
        if not isinstance(env_config, dict):
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message="'env_config' must be a dictionary",
                    path=self.settings_file,
                )
            )
            return

        # Check that base is declared
        if "base" not in env_config:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.WARNING,
                    message="'base' environment not declared in env_config",
                    path=self.settings_file,
                    suggestion="Add 'base' to env_config with paths to base configuration files",
                )
            )

    def _validate_environments(self) -> None:
        """Validate each environment has proper structure"""
        if not self.config_dir.exists():
            return

        # Get list of environment directories
        env_dirs = [
            d.name for d in self.config_dir.iterdir() if d.is_dir() and not d.name.startswith(".")
        ]

        # Also add 'base' if it has files in config/ root
        canonical_envs = set(env_dirs)
        if any(
            (self.config_dir / f"global_settings{ext}").exists()
            for ext in self.SUPPORTED_EXTENSIONS
        ):
            canonical_envs.add("base")

        # Validate each environment
        for env in canonical_envs:
            if env == "base":
                # Base files are in config/ root
                continue

            env_path = self.config_dir / env
            if not env_path.exists():
                self.errors.append(
                    ValidationError(
                        severity=ValidationSeverity.WARNING,
                        message=f"Environment '{env}' is empty (no files)",
                        path=env_path,
                        suggestion=f"Add configuration files to {env_path}",
                    )
                )

    def _validate_fallback_chains(self) -> None:
        """Validate fallback chains are complete"""
        if not self.settings_file or not self.settings_file.exists():
            return

        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                settings = json.load(f)
        except Exception as e:
            logger.debug(f"Failed to load settings file {self.settings_file}: {e}")
            return

        env_config = settings.get("env_config", {})
        declared_envs = list(env_config.keys())

        for env in declared_envs:
            try:
                chain = get_fallback_chain(env)

                # Check that all environments in chain are declared or base
                for fallback_env in chain:
                    if fallback_env not in declared_envs and fallback_env != "base":
                        self.errors.append(
                            ValidationError(
                                severity=ValidationSeverity.WARNING,
                                environment=env,
                                message=f"Fallback environment '{fallback_env}' not declared",
                                path=self.config_dir,
                                suggestion=f"Add configuration for '{fallback_env}' or remove reference",
                            )
                        )
            except ValueError as e:
                # Unknown environment
                logger.debug(f"Error getting fallback chain for {env}: {e}")
                self.errors.append(
                    ValidationError(
                        severity=ValidationSeverity.WARNING,
                        environment=env,
                        message=f"Unknown environment '{env}' may not have proper fallback chain",
                        path=self.config_dir,
                    )
                )

    def _validate_config_syntax(self) -> None:
        """Validate configuration files have valid syntax"""
        if not self.config_dir.exists():
            return

        # Find all config files
        config_files = list(self.config_dir.rglob("*.*"))

        for config_file in config_files:
            if config_file.suffix == ".json":
                self._validate_json_file(config_file)
            elif config_file.suffix in [".yaml", ".yml"]:
                self._validate_yaml_file(config_file)
            # DSL files checked separately if needed

    def _validate_json_file(self, file_path: Path) -> None:
        """Validate JSON file syntax"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                json.load(f)
        except json.JSONDecodeError as e:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message=f"Invalid JSON syntax: {e}",
                    path=file_path,
                    suggestion="Fix JSON syntax errors",
                )
            )
        except Exception as e:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message=f"Error reading JSON file: {e}",
                    path=file_path,
                )
            )

    def _validate_yaml_file(self, file_path: Path) -> None:
        """Validate YAML file syntax"""
        try:
            import yaml

            with open(file_path, "r", encoding="utf-8") as f:
                yaml.safe_load(f)
        except ImportError:
            # YAML not installed, skip
            logger.debug("PyYAML not installed, skipping YAML validation")
        except Exception as e:
            self.errors.append(
                ValidationError(
                    severity=ValidationSeverity.ERROR,
                    message=f"Invalid YAML syntax: {e}",
                    path=file_path,
                    suggestion="Fix YAML syntax errors",
                )
            )

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of validation results.

        Returns:
            Dictionary with counts by severity
        """
        summary = {
            "total": len(self.errors),
            "critical": sum(1 for e in self.errors if e.severity == ValidationSeverity.CRITICAL),
            "errors": sum(1 for e in self.errors if e.severity == ValidationSeverity.ERROR),
            "warnings": sum(1 for e in self.errors if e.severity == ValidationSeverity.WARNING),
            "info": sum(1 for e in self.errors if e.severity == ValidationSeverity.INFO),
            "is_valid": all(e.severity != ValidationSeverity.CRITICAL for e in self.errors),
        }
        return summary

    def print_report(self) -> None:
        """Print human-readable validation report"""
        summary = self.get_summary()

        print("\n" + "=" * 70)
        print("Environment Configuration Validation Report")
        print(f"Project: {self.project_path}")
        print("=" * 70)

        if summary["total"] == 0:
            print("✓ All checks passed!")
        else:
            print(f"\nFound {summary['total']} issues:")
            print(f"  • Critical: {summary['critical']}")
            print(f"  • Errors:   {summary['errors']}")
            print(f"  • Warnings: {summary['warnings']}")
            print(f"  • Info:     {summary['info']}")
            print("\nDetails:")

            # Group by severity
            for severity in [
                ValidationSeverity.CRITICAL,
                ValidationSeverity.ERROR,
                ValidationSeverity.WARNING,
                ValidationSeverity.INFO,
            ]:
                errors_by_severity = [e for e in self.errors if e.severity == severity]
                if errors_by_severity:
                    print(f"\n{severity.upper()}:")
                    for error in errors_by_severity:
                        print(f"  • {error}")

        print("\n" + "=" * 70 + "\n")


__all__ = [
    "ValidationSeverity",
    "ValidationError",
    "EnvironmentConfigValidator",
]
