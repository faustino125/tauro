from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Ajusta DATABASE_URL según tu configuración (env var o settings)
DATABASE_URL = "sqlite:///./tauro.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
    if DATABASE_URL.startswith("sqlite")
    else {},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# helper typing alias (opcional)
def get_session() -> Session:
    return SessionLocal()
