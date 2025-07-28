from .constants import (
    PipelineType,
    StreamingMode,
    StreamingTrigger,
    StreamingFormat,
    StreamingOutputMode,
    DEFAULT_STREAMING_CONFIG,
)

from .exceptions import (
    StreamingError,
    StreamingValidationError,
    StreamingFormatNotSupportedError,
    StreamingQueryError,
    StreamingPipelineError,
)

from .validators import StreamingValidator

from .readers import (
    StreamingReaderFactory,
    BaseStreamingReader,
    KafkaStreamingReader,
    DeltaStreamingReader,
    FileStreamingReader,
    KinesisStreamingReader,
    SocketStreamingReader,
    RateStreamingReader,
    MemoryStreamingReader,
)

from .writers import (
    StreamingWriterFactory,
    BaseStreamingWriter,
    ConsoleStreamingWriter,
    DeltaStreamingWriter,
    ParquetStreamingWriter,
    KafkaStreamingWriter,
    MemoryStreamingWriter,
    ForeachBatchStreamingWriter,
    JSONStreamingWriter,
)

from .query_manager import StreamingQueryManager
from .pipeline_manager import StreamingPipelineManager

# Importaciones de integración con el framework base
from ..config.streaming_context import StreamingContext
from ..exec.executor import PipelineExecutor

__version__ = "1.0.0"
__author__ = "Tauro Team"

__all__ = [
    # Constantes y tipos
    "PipelineType",
    "StreamingMode",
    "StreamingTrigger",
    "StreamingFormat",
    "StreamingOutputMode",
    "DEFAULT_STREAMING_CONFIG",
    # Excepciones
    "StreamingError",
    "StreamingValidationError",
    "StreamingFormatNotSupportedError",
    "StreamingQueryError",
    "StreamingPipelineError",
    # Validadores
    "StreamingValidator",
    # Readers
    "StreamingReaderFactory",
    "BaseStreamingReader",
    "KafkaStreamingReader",
    "DeltaStreamingReader",
    "FileStreamingReader",
    "KinesisStreamingReader",
    "SocketStreamingReader",
    "RateStreamingReader",
    "MemoryStreamingReader",
    # Writers
    "StreamingWriterFactory",
    "BaseStreamingWriter",
    "ConsoleStreamingWriter",
    "DeltaStreamingWriter",
    "ParquetStreamingWriter",
    "KafkaStreamingWriter",
    "MemoryStreamingWriter",
    "ForeachBatchStreamingWriter",
    "JSONStreamingWriter",
    # Gestores
    "StreamingQueryManager",
    "StreamingPipelineManager",
    # Contexto y ejecución
    "StreamingContext",
    "PipelineExecutor",
]


def get_version() -> str:
    """Retorna la versión del módulo streaming."""
    return __version__


def list_supported_streaming_formats() -> dict:
    """Lista todos los formatos streaming soportados."""
    return {
        "input_formats": [fmt.value for fmt in StreamingFormat],
        "output_formats": [
            "console",
            "delta",
            "parquet",
            "kafka",
            "memory",
            "foreachBatch",
            "json",
        ],
        "trigger_types": [trigger.value for trigger in StreamingTrigger],
        "output_modes": [mode.value for mode in StreamingOutputMode],
    }


def validate_streaming_requirements():
    """Valida que los requisitos para streaming estén disponibles."""
    import sys

    requirements = {
        "pyspark": "Requerido para streaming engine",
        "kafka": "Opcional - para soporte Kafka",
        "delta": "Opcional - para Delta Lake streaming",
    }

    results = {}

    # Validar PySpark
    try:
        import pyspark  # type: ignore

        results["pyspark"] = {"available": True, "version": pyspark.__version__}
    except ImportError:
        results["pyspark"] = {"available": False, "error": "PySpark no está instalado"}

    # Validar Kafka (opcional)
    try:
        # Verificar si Kafka está disponible en Spark
        results["kafka"] = {
            "available": True,
            "note": "Verificar kafka-clients en classpath",
        }
    except Exception:
        results["kafka"] = {"available": False, "note": "Kafka support not verified"}

    # Validar Delta (opcional)
    try:
        import delta  # type: ignore

        results["delta"] = {
            "available": True,
            "version": getattr(delta, "__version__", "unknown"),
        }
    except ImportError:
        results["delta"] = {"available": False, "error": "Delta Lake no está instalado"}

    return results


def print_streaming_summary():
    """Imprime un resumen de las capacidades streaming."""

    print("🌊 TAURO STREAMING MODULE")
    print("=" * 50)
    print(f"Version: {__version__}")
    print("\n📋 Supported Features:")

    formats = list_supported_streaming_formats()

    print(f"  • Input formats: {', '.join(formats['input_formats'])}")
    print(f"  • Output formats: {', '.join(formats['output_formats'])}")
    print(f"  • Trigger types: {', '.join(formats['trigger_types'])}")
    print(f"  • Output modes: {', '.join(formats['output_modes'])}")

    print("\n🔧 Requirements Check:")
    requirements = validate_streaming_requirements()

    for component, info in requirements.items():
        status = "✅" if info["available"] else "❌"
        version_info = (
            f" (v{info.get('version', 'unknown')})" if info.get("version") else ""
        )
        print(f"  {status} {component}{version_info}")

        if not info["available"] and "error" in info:
            print(f"      Error: {info['error']}")
        elif "note" in info:
            print(f"      Note: {info['note']}")

    print("\n📚 Quick Start:")
    print("  1. Create streaming configuration")
    print("  2. Use StreamingContext.from_python_dsl()")
    print("  3. Initialize EnhancedPipelineExecutor()")
    print("  4. Run streaming pipeline with execution_mode='async'")
    print("\n  See examples in tauro.streaming.examples for detailed usage.")


# Verificación automática al importar (opcional)
if __name__ == "__main__":
    print_streaming_summary()


# Configuración de logging específica para streaming
def configure_streaming_logging():
    """Configura logging específico para componentes streaming."""
    from loguru import logger  # type: ignore
    import sys

    # Formato específico para streaming con identificador de query
    streaming_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>STREAMING</cyan> | "
        "<blue>{name}</blue>:<blue>{function}</blue>:<blue>{line}</blue> | "
        "<level>{message}</level>"
    )

    # Agregar handler específico para streaming
    logger.add(
        sys.stdout,
        format=streaming_format,
        filter=lambda record: "streaming" in record["name"].lower(),
        level="INFO",
    )

    # Configurar niveles específicos
    logger.level("STREAM_DEBUG", no=15, color="<blue>")
    logger.level("STREAM_METRICS", no=25, color="<magenta>")


# Auto-configurar logging si se está ejecutando en modo streaming
import os

if os.getenv("TAURO_STREAMING_LOGGING", "false").lower() == "true":
    configure_streaming_logging()
