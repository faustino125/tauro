# Evita ValueError: I/O operation on closed file producido por Loguru cuando hilos siguen
# escribiendo tras el cierre de los streams de pytest.
#
# Coloca este archivo en la raíz del repo o en el directorio de tests para que pytest lo cargue.
from loguru import logger


def pytest_configure(config):
    """
    Reconfigura loguru al inicio de la sesión de tests:
    - Elimina sinks existentes (que pueden apuntar a streams que pytest cerrará).
    - Añade un sink "no-op" que traga todos los mensajes (evita excepciones en hilos).
    Si quieres ver logs durante los tests, reemplaza la lambda por logger.add(sys.__stdout__).
    """
    try:
        logger.remove()
    except Exception:
        # ignore if no sinks or error removing
        pass

    # Sink que descarta todos los mensajes (rápido y seguro para tests con hilos)
    logger.add(lambda msg: None, level="INFO", enqueue=True)
