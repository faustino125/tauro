"""
Tests para verificar que todas las exportaciones del módulo streaming funcionan correctamente.
"""
import pytest


class TestStreamingExports:
    """Tests para validar las exportaciones del módulo streaming."""

    def test_constants_exports(self):
        """Verificar que todas las constantes y enums se exportan correctamente."""
        from core.streaming import (
            PipelineType,
            StreamingMode,
            StreamingTrigger,
            StreamingFormat,
            StreamingOutputMode,
            DEFAULT_STREAMING_CONFIG,
            STREAMING_FORMAT_CONFIGS,
            STREAMING_VALIDATIONS,
        )

        # Verificar que son tipos correctos
        assert hasattr(PipelineType, "STREAMING")
        assert hasattr(StreamingMode, "CONTINUOUS")
        assert hasattr(StreamingTrigger, "PROCESSING_TIME")
        assert hasattr(StreamingFormat, "KAFKA")
        assert hasattr(StreamingOutputMode, "APPEND")
        assert isinstance(DEFAULT_STREAMING_CONFIG, dict)
        assert isinstance(STREAMING_FORMAT_CONFIGS, dict)
        assert isinstance(STREAMING_VALIDATIONS, dict)

    def test_exceptions_exports(self):
        """Verificar que todas las excepciones se exportan correctamente."""
        from core.streaming import (
            StreamingError,
            StreamingValidationError,
            StreamingFormatNotSupportedError,
            StreamingQueryError,
            StreamingPipelineError,
            StreamingConnectionError,
            StreamingConfigurationError,
            StreamingTimeoutError,
            StreamingResourceError,
            handle_streaming_error,
            create_error_context,
        )

        # Verificar que son clases de excepciones
        assert issubclass(StreamingError, Exception)
        assert issubclass(StreamingValidationError, StreamingError)
        assert issubclass(StreamingFormatNotSupportedError, StreamingError)
        assert issubclass(StreamingQueryError, StreamingError)
        assert issubclass(StreamingPipelineError, StreamingError)
        assert issubclass(StreamingConnectionError, StreamingError)
        assert issubclass(StreamingConfigurationError, StreamingError)
        assert issubclass(StreamingTimeoutError, StreamingError)
        assert issubclass(StreamingResourceError, StreamingError)

        # Verificar que son callables
        assert callable(handle_streaming_error)
        assert callable(create_error_context)

    def test_validators_exports(self):
        """Verificar que el validador se exporta correctamente."""
        from core.streaming import StreamingValidator

        # Verificar que es una clase
        assert isinstance(StreamingValidator, type)

        # Verificar que tiene métodos clave
        validator = StreamingValidator()
        assert hasattr(validator, "validate_streaming_pipeline_config")
        assert hasattr(validator, "validate_streaming_node_config")

    def test_readers_exports(self):
        """Verificar que todos los readers se exportan correctamente."""
        from core.streaming import (
            BaseStreamingReader,
            StreamingReaderFactory,
            KafkaStreamingReader,
            DeltaStreamingReader,
            RateStreamingReader,
        )

        # Verificar que son clases
        assert isinstance(BaseStreamingReader, type)
        assert isinstance(StreamingReaderFactory, type)
        assert isinstance(KafkaStreamingReader, type)
        assert isinstance(DeltaStreamingReader, type)
        assert isinstance(RateStreamingReader, type)

        # Verificar herencia
        assert issubclass(KafkaStreamingReader, BaseStreamingReader)
        assert issubclass(DeltaStreamingReader, BaseStreamingReader)
        assert issubclass(RateStreamingReader, BaseStreamingReader)

    def test_writers_exports(self):
        """Verificar que todos los writers se exportan correctamente."""
        from core.streaming import (
            BaseStreamingWriter,
            StreamingWriterFactory,
            ConsoleStreamingWriter,
            DeltaStreamingWriter,
            ParquetStreamingWriter,
            KafkaStreamingWriter,
            MemoryStreamingWriter,
            ForeachBatchStreamingWriter,
            JSONStreamingWriter,
            CSVStreamingWriter,
        )

        # Verificar que son clases
        assert isinstance(BaseStreamingWriter, type)
        assert isinstance(StreamingWriterFactory, type)
        assert isinstance(ConsoleStreamingWriter, type)
        assert isinstance(DeltaStreamingWriter, type)
        assert isinstance(ParquetStreamingWriter, type)
        assert isinstance(KafkaStreamingWriter, type)
        assert isinstance(MemoryStreamingWriter, type)
        assert isinstance(ForeachBatchStreamingWriter, type)
        assert isinstance(JSONStreamingWriter, type)
        assert isinstance(CSVStreamingWriter, type)

        # Verificar herencia
        assert issubclass(ConsoleStreamingWriter, BaseStreamingWriter)
        assert issubclass(DeltaStreamingWriter, BaseStreamingWriter)
        assert issubclass(ParquetStreamingWriter, BaseStreamingWriter)
        assert issubclass(KafkaStreamingWriter, BaseStreamingWriter)
        assert issubclass(MemoryStreamingWriter, BaseStreamingWriter)
        assert issubclass(ForeachBatchStreamingWriter, BaseStreamingWriter)
        assert issubclass(JSONStreamingWriter, BaseStreamingWriter)
        assert issubclass(CSVStreamingWriter, BaseStreamingWriter)

    def test_managers_exports(self):
        """Verificar que los managers se exportan correctamente."""
        from core.streaming import (
            StreamingQueryManager,
            StreamingPipelineManager,
        )

        # Verificar que son clases
        assert isinstance(StreamingQueryManager, type)
        assert isinstance(StreamingPipelineManager, type)

    def test_all_attribute(self):
        """Verificar que __all__ contiene todas las exportaciones esperadas."""
        import core.streaming

        # Verificar que __all__ existe
        assert hasattr(core.streaming, "__all__")
        all_exports = core.streaming.__all__

        # Verificar que es una lista
        assert isinstance(all_exports, list)

        # Verificar conteo esperado
        # 8 constants + 11 exceptions (9 clases + 2 funciones) + 1 validator +
        # 5 readers (1 base + 1 factory + 3 implementations) +
        # 10 writers (1 base + 1 factory + 8 implementations) + 2 managers = 37
        expected_count = 37
        assert (
            len(all_exports) == expected_count
        ), f"Expected {expected_count} exports, got {len(all_exports)}"

        # Verificar que no hay duplicados
        assert len(all_exports) == len(set(all_exports)), "Found duplicate entries in __all__"

        # Verificar que todas las entradas en __all__ son exportables
        for name in all_exports:
            assert hasattr(
                core.streaming, name
            ), f"'{name}' is in __all__ but not exported from module"

    def test_import_all_wildcard(self):
        """Verificar que la importación con wildcard funciona correctamente."""
        # Usar un namespace separado para evitar contaminación
        namespace = {}
        exec("from core.streaming import *", namespace)

        # Verificar que se importaron las clases principales
        assert "StreamingQueryManager" in namespace
        assert "StreamingPipelineManager" in namespace
        assert "StreamingValidator" in namespace
        assert "StreamingError" in namespace
        assert "StreamingFormat" in namespace

        # Verificar que no se importaron nombres privados
        for name in namespace:
            if name.startswith("_"):
                assert name in [
                    "__builtins__",
                    "__name__",
                    "__doc__",
                ], f"Private name '{name}' should not be exported"

    def test_exception_hierarchy(self):
        """Verificar la jerarquía completa de excepciones."""
        from core.streaming import (
            StreamingError,
            StreamingValidationError,
            StreamingFormatNotSupportedError,
            StreamingQueryError,
            StreamingPipelineError,
            StreamingConnectionError,
            StreamingConfigurationError,
            StreamingTimeoutError,
            StreamingResourceError,
        )

        # Crear instancias para verificar
        base_error = StreamingError("Base error")
        assert str(base_error) == "Base error"

        validation_error = StreamingValidationError("Validation failed", field="test")
        assert "field" in validation_error.context

        format_error = StreamingFormatNotSupportedError(
            "Format not supported", format_name="custom"
        )
        assert "format_name" in format_error.context

    def test_factory_pattern(self):
        """Verificar que el patrón factory funciona correctamente."""
        from unittest.mock import Mock
        from core.streaming import (
            StreamingReaderFactory,
            StreamingWriterFactory,
        )

        # Mock context con spark
        context = Mock()
        context.spark = Mock()

        # Test reader factory
        reader_factory = StreamingReaderFactory(context)
        assert hasattr(reader_factory, "get_reader")
        assert hasattr(reader_factory, "list_supported_formats")

        supported_readers = reader_factory.list_supported_formats()
        assert isinstance(supported_readers, list)
        assert "kafka" in supported_readers
        assert "delta_stream" in supported_readers

        # Test writer factory
        writer_factory = StreamingWriterFactory(context)
        assert hasattr(writer_factory, "get_writer")
        assert hasattr(writer_factory, "list_supported_formats")

        supported_writers = writer_factory.list_supported_formats()
        assert isinstance(supported_writers, list)
        assert "console" in supported_writers
        assert "delta" in supported_writers

    def test_docstring_present(self):
        """Verificar que el módulo tiene docstring."""
        import core.streaming

        assert core.streaming.__doc__ is not None
        assert len(core.streaming.__doc__) > 0
        assert "Tauro Streaming" in core.streaming.__doc__

    def test_base_classes_abstract(self):
        """Verificar que las clases base son abstractas."""
        from abc import ABC
        from core.streaming import BaseStreamingReader, BaseStreamingWriter

        # Verificar que heredan de ABC
        assert issubclass(BaseStreamingReader, ABC)
        assert issubclass(BaseStreamingWriter, ABC)

        # Verificar que no se pueden instanciar directamente
        from unittest.mock import Mock

        context = Mock()

        with pytest.raises(TypeError):
            BaseStreamingReader(context)

        with pytest.raises(TypeError):
            BaseStreamingWriter(context)

    def test_utility_functions(self):
        """Verificar que las funciones de utilidad funcionan correctamente."""
        from core.streaming import handle_streaming_error, create_error_context

        # Test create_error_context
        context = create_error_context(
            operation="test_operation", component="TestComponent", node_name="test_node"
        )

        assert isinstance(context, dict)
        assert context["operation"] == "test_operation"
        assert context["component"] == "TestComponent"
        assert context["node_name"] == "test_node"
        assert "timestamp" in context

        # Test handle_streaming_error como decorador
        @handle_streaming_error
        def test_function():
            raise ValueError("Test error")

        from core.streaming import StreamingError

        with pytest.raises(StreamingError) as exc_info:
            test_function()

        assert "Test error" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
