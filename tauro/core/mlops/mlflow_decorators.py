"""
Decoradores y helpers convenientes para usar MLflow en nodos de Tauro.

Simplifica el logging de MLflow con decoradores pythónicos.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional
import time

from loguru import logger

try:
    import mlflow  # type: ignore

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False


def _mlf_log_params_from_call(
    func: Callable, args, kwargs, log_params: Optional[List[str]]
) -> None:
    if not log_params:
        return
    try:
        import inspect

        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        for param_name in log_params:
            if param_name in bound_args.arguments:
                value = bound_args.arguments[param_name]
                try:
                    mlflow.log_param(f"{func.__name__}_{param_name}", value)
                except Exception as e:
                    logger.debug(f"Could not log param {param_name}: {e}")
    except Exception as e:
        logger.debug(f"Could not bind or log params: {e}")


def _mlf_log_execution_time(func_name: str, duration: float) -> None:
    try:
        mlflow.log_metric(f"{func_name}_execution_time", duration)
    except Exception as e:
        logger.debug(f"Could not log execution time: {e}")


def _mlf_log_result_metric(name: str, value: Any) -> None:
    try:
        mlflow.log_metric(name, value)
    except Exception as e:
        logger.debug(f"Could not log result: {e}")


def mlflow_track(
    log_params: Optional[List[str]] = None,
    log_result_as: Optional[str] = None,
    log_execution_time: bool = True,
):
    """
    Decorator para tracking automático de funciones con MLflow.

    Args:
        log_params: Lista de nombres de parámetros a loggear
        log_result_as: Nombre para loggear el resultado (si es numérico)
        log_execution_time: Loggear tiempo de ejecución

    Example:
        >>> @mlflow_track(log_params=["n_estimators", "max_depth"],
        ...               log_result_as="accuracy")
        ... def train_model(n_estimators, max_depth, **kwargs):
        ...     # Tu código aquí
        ...     return 0.95
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if not MLFLOW_AVAILABLE:
                return func(*args, **kwargs)

            # Log parámetros seleccionados (delegado a helper)
            _mlf_log_params_from_call(func, args, kwargs, log_params)

            # Ejecutar función con timing
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            duration = time.perf_counter() - start_time

            # Log execution time (delegado a helper)
            if log_execution_time:
                _mlf_log_execution_time(func.__name__, duration)

            # Log resultado si es numérico (delegado a helper)
            if log_result_as and isinstance(result, (int, float)):
                _mlf_log_result_metric(log_result_as, result)

            return result

        return wrapper

    return decorator


def log_dataframe_stats(df: Any, prefix: str = "df") -> None:
    """
    Log estadísticas básicas de un DataFrame a MLflow.

    Args:
        df: pandas o Spark DataFrame
        prefix: Prefijo para las métricas

    Example:
        >>> log_dataframe_stats(df, prefix="train")
        # Loggea: train_rows, train_cols, train_nulls, etc.
    """
    if not MLFLOW_AVAILABLE:
        return

    try:
        # Detectar tipo de DataFrame
        if hasattr(df, "toPandas"):
            # Spark DataFrame
            mlflow.log_metric(f"{prefix}_rows", df.count())
            mlflow.log_metric(f"{prefix}_cols", len(df.columns))
        else:
            # Pandas DataFrame
            mlflow.log_metric(f"{prefix}_rows", len(df))
            mlflow.log_metric(f"{prefix}_cols", len(df.columns))

            # Nulls
            null_count = df.isnull().sum().sum()
            mlflow.log_metric(f"{prefix}_nulls", int(null_count))

            # Memory usage (MB)
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            mlflow.log_metric(f"{prefix}_memory_mb", memory_mb)

    except Exception as e:
        logger.debug(f"Could not log DataFrame stats: {e}")


def log_model_metrics(
    y_true: Any,
    y_pred: Any,
    prefix: str = "test",
    task_type: str = "classification",
) -> Dict[str, float]:
    """
    Log métricas estándar de ML a MLflow.

    Args:
        y_true: True labels
        y_pred: Predicted labels
        prefix: Prefijo para las métricas
        task_type: "classification" o "regression"

    Returns:
        Dictionary con las métricas calculadas

    Example:
        >>> metrics = log_model_metrics(y_test, y_pred, prefix="test")
        # Loggea: test_accuracy, test_precision, etc.
    """
    if not MLFLOW_AVAILABLE:
        return {}

    metrics = {}

    try:
        if task_type == "classification":
            from sklearn.metrics import (  # type: ignore
                accuracy_score,
                precision_score,
                recall_score,
                f1_score,
            )

            metrics[f"{prefix}_accuracy"] = accuracy_score(y_true, y_pred)
            metrics[f"{prefix}_precision"] = precision_score(
                y_true, y_pred, average="weighted", zero_division=0
            )
            metrics[f"{prefix}_recall"] = recall_score(
                y_true, y_pred, average="weighted", zero_division=0
            )
            metrics[f"{prefix}_f1"] = f1_score(y_true, y_pred, average="weighted", zero_division=0)

        else:  # regression
            from sklearn.metrics import (  # type: ignore
                mean_squared_error,
                mean_absolute_error,
                r2_score,
            )

            metrics[f"{prefix}_mse"] = mean_squared_error(y_true, y_pred)
            metrics[f"{prefix}_rmse"] = mean_squared_error(y_true, y_pred) ** 0.5
            metrics[f"{prefix}_mae"] = mean_absolute_error(y_true, y_pred)
            metrics[f"{prefix}_r2"] = r2_score(y_true, y_pred)

        # Log todas las métricas
        for key, value in metrics.items():
            mlflow.log_metric(key, value)

    except Exception as e:
        logger.warning(f"Could not log model metrics: {e}")

    return metrics


def log_confusion_matrix(
    y_true: Any,
    y_pred: Any,
    labels: Optional[List[str]] = None,
    filename: str = "confusion_matrix.png",
) -> None:
    """
    Log confusion matrix como artifact en MLflow.

    Args:
        y_true: True labels
        y_pred: Predicted labels
        labels: Label names (optional)
        filename: Nombre del archivo a guardar

    Example:
        >>> log_confusion_matrix(y_test, y_pred, labels=["cat", "dog"])
    """
    if not MLFLOW_AVAILABLE:
        return

    try:
        import matplotlib.pyplot as plt  # type: ignore
        import seaborn as sns  # type: ignore
        from sklearn.metrics import confusion_matrix  # type: ignore

        cm = confusion_matrix(y_true, y_pred)

        plt.figure(figsize=(8, 6))
        sns.heatmap(
            cm,
            annot=True,
            fmt="d",
            cmap="Blues",
            xticklabels=labels,
            yticklabels=labels,
        )
        plt.title("Confusion Matrix")
        plt.ylabel("True Label")
        plt.xlabel("Predicted Label")
        plt.tight_layout()

        mlflow.log_figure(plt.gcf(), filename)
        plt.close()

    except Exception as e:
        logger.warning(f"Could not log confusion matrix: {e}")


def log_feature_importance(
    model: Any,
    feature_names: List[str],
    top_n: int = 10,
    filename: str = "feature_importance.png",
) -> None:
    """
    Log feature importance como artifact en MLflow.

    Args:
        model: Modelo con atributo feature_importances_
        feature_names: Nombres de las features
        top_n: Número de features a mostrar
        filename: Nombre del archivo

    Example:
        >>> log_feature_importance(model, X.columns.tolist())
    """
    if not MLFLOW_AVAILABLE:
        return

    try:
        import matplotlib.pyplot as plt  # type: ignore
        import pandas as pd  # type: ignore

        # Extraer importances
        if hasattr(model, "feature_importances_"):
            importances = model.feature_importances_
        elif hasattr(model, "coef_"):
            importances = abs(model.coef_[0])
        else:
            logger.warning("Model does not have feature_importances_ or coef_")
            return

        # Create DataFrame y ordenar
        df = (
            pd.DataFrame(
                {
                    "feature": feature_names,
                    "importance": importances,
                }
            )
            .sort_values("importance", ascending=False)
            .head(top_n)
        )

        # Plot
        plt.figure(figsize=(10, 6))
        plt.barh(df["feature"], df["importance"])
        plt.xlabel("Importance")
        plt.title(f"Top {top_n} Feature Importances")
        plt.tight_layout()

        mlflow.log_figure(plt.gcf(), filename)
        plt.close()

        # Log también como métricas
        for _, row in df.iterrows():
            mlflow.log_metric(f"importance_{row['feature'].replace(' ', '_')}", row["importance"])

    except Exception as e:
        logger.warning(f"Could not log feature importance: {e}")


def log_training_curve(
    train_scores: List[float],
    val_scores: Optional[List[float]] = None,
    metric_name: str = "accuracy",
    filename: str = "training_curve.png",
) -> None:
    """
    Log curva de entrenamiento como artifact en MLflow.

    Args:
        train_scores: Scores de training por época
        val_scores: Scores de validación por época (optional)
        metric_name: Nombre de la métrica
        filename: Nombre del archivo

    Example:
        >>> log_training_curve([0.7, 0.8, 0.85], [0.65, 0.75, 0.82], "accuracy")
    """
    if not MLFLOW_AVAILABLE:
        return

    try:
        import matplotlib.pyplot as plt  # type: ignore

        epochs = range(1, len(train_scores) + 1)

        plt.figure(figsize=(10, 6))
        plt.plot(epochs, train_scores, label=f"Train {metric_name}", marker="o")

        if val_scores:
            plt.plot(epochs, val_scores, label=f"Val {metric_name}", marker="s")

        plt.xlabel("Epoch")
        plt.ylabel(metric_name.capitalize())
        plt.title(f"Training Curve - {metric_name}")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        mlflow.log_figure(plt.gcf(), filename)
        plt.close()

        # Log también las métricas por época
        for epoch, score in enumerate(train_scores, 1):
            mlflow.log_metric(f"train_{metric_name}", score, step=epoch)

        if val_scores:
            for epoch, score in enumerate(val_scores, 1):
                mlflow.log_metric(f"val_{metric_name}", score, step=epoch)

    except Exception as e:
        logger.warning(f"Could not log training curve: {e}")


class MLflowNodeContext:
    """
    Context manager conveniente para logging en nodos.

    Example:
        >>> with MLflowNodeContext("train") as mlf:
        ...     mlf.log_param("n_estimators", 100)
        ...     mlf.log_metric("accuracy", 0.95)
        ...     mlf.log_model(model, "model")
    """

    def __init__(self, node_name: str):
        self.node_name = node_name
        self.start_time = None

    def __enter__(self):
        if MLFLOW_AVAILABLE:
            self.start_time = time.perf_counter()
            mlflow.log_param("node_name", self.node_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if MLFLOW_AVAILABLE and self.start_time:
            duration = time.perf_counter() - self.start_time
            mlflow.log_metric(f"{self.node_name}_duration", duration)

            if exc_type:
                mlflow.log_param(f"{self.node_name}_error", str(exc_val))

    def log_param(self, key: str, value: Any) -> None:
        """Log parameter."""
        if MLFLOW_AVAILABLE:
            mlflow.log_param(key, value)

    def log_metric(self, key: str, value: float, step: int = 0) -> None:
        """Log metric."""
        if MLFLOW_AVAILABLE:
            mlflow.log_metric(key, value, step=step)

    def log_model(self, model: Any, artifact_path: str, **kwargs) -> None:
        """Log model."""
        if MLFLOW_AVAILABLE:
            # Auto-detect flavor
            if hasattr(model, "fit") and hasattr(model, "predict"):
                mlflow.sklearn.log_model(model, artifact_path, **kwargs)
            else:
                mlflow.pyfunc.log_model(artifact_path, python_model=model, **kwargs)
