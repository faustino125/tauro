# tauro
Es un framework avanzado para la ejecución de pipelines de datos que combina automatización, escalabilidad y flexibilidad. Diseñado para simplificar la gestión de flujos de trabajo complejos de datos, desde ETL tradicionales hasta pipelines de Machine Learning.

```mermaid
graph TD
    A[CLI Input] --> B[Parse Arguments]
    B --> C[Config Discovery]
    C --> D[Load Configuration]
    D --> E[Initialize Context]
    E --> F[Create Spark Session]
    F --> G[Pipeline Validation]
    G --> H[Dependency Resolution]
    H --> I[Parallel Node Execution]
    I --> J[Output Management]
