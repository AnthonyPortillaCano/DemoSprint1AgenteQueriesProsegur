# Agente Inteligente para la Generación Automática de Queries MongoDB a partir de Lenguaje Natural

Proyecto de investigación (Maestría en IA – UNI) para entrenar y probar un agente inteligente capaz de transformar instrucciones en lenguaje natural en queries MongoDB, incluyendo soporte para joins dinámicos y análisis de datos.

Este repositorio sirve como guía para organizar proyectos de agentes generadores de queries y EDA en MongoDB.

Este repositorio forma parte del curso **Proyecto de Investigación II (MIA 402)**.

---

## 👥 Autores
- Anthony Portilla Cano – [@AnthonyPortillaCano](https://github.com/AnthonyPortillaCano)
- Equipo de Maestría UNI

---

## 📊 Dataset
- **Fuente**: Dataset sintético de transacciones y empleados/departamentos para pruebas de queries MongoDB
- **Registros**: 10+ ejemplos manuales y generados
- **Variables**: fecha, dispositivos, sucursal, puntos de servicio, ciclos de envío, transacciones, monto, moneda, empleados, departamentos
- **Versión usada**: actualizada el 02/10/2025
- **Archivo principal**: `datasets/transactions_collection.json`

---

## 🗂️ Estructura del repositorio
```
datasets/
 └── transactions_collection.json   # datos de ejemplo para queries y EDA
logs/
notebooks/
 ├── EDA_Semana5.ipynb              # Análisis exploratorio y validación de datos
 └── AgenteInteligente_QueriesMongoDB.ipynb   # Pruebas del agente generador de queries
src/
 ├── AgenteGeneradorQueryMongo.py   # agente inteligente para generación de queries
 ├── dataset_manager.py             # gestor de dataset y esquemas
 ├── llm_suggestion_engine.py       # motor de sugerencias LLM
 └── main.py                        # script principal
README.md
requirements.txt
```

---

## ⚙️ Requisitos
Instalar dependencias usando `pip`:
```bash
pip install -r requirements.txt
```
- Python 3.10 o superior (recomendado 3.11.x, probado en 3.11.7)
- Compatible con Windows, Linux o MacOS

Puedes verificar tu versión de Python ejecutando:
```powershell
python --version
```
Ejemplo de salida esperada:
```
Python 3.11.7
```

## 🚀 Cómo ejecutar el pipeline
1. **Validar datos y EDA**
  - Abrir y ejecutar el notebook `notebooks/EDA_Semana5.ipynb` para explorar los datos y verificar ejemplos.

2. **Probar agente generador de queries**
   - Abrir y ejecutar el notebook `notebooks/AgenteInteligente_QueriesMongoDB.ipynb`.
   - Modificar la variable `instruccion` para probar diferentes frases en lenguaje natural.
   - Validar que el agente genera queries correctas, incluyendo instrucciones con joins y proyecciones dinámicas.

3. **Ejecutar scripts principales**
   - Ejecutar `src/main.py` para pruebas integradas.
    - Para iniciar el API (FastAPI) y la web (Streamlit), usa los siguientes comandos:

      ```powershell
      # Ejecutar API (FastAPI)
      uvicorn src.main:app --reload

      # Ejecutar la web (Streamlit)
      streamlit run src/app.py
      ```
---
## Variables de entorno

Si usas claves API u otras variables sensibles, crea un archivo `.env` en la raíz del proyecto con el siguiente formato:

```
AZURE_OPENAI_API_KEY=tu_clave
AZURE_OPENAI_ENDPOINT=tu_endpoint
AZURE_OPENAI_DEPLOYMENT=tu_deployment
```


También puedes abrir el notebook principal en la carpeta `notebooks/` y seguir los ejemplos para generar queries MongoDB desde lenguaje natural.


## 📈 Resultados esperados (Semana 3)
- **EDA inicial** en `notebooks/EDA_Semana5.ipynb`.
- **Agente genera queries MongoDB** a partir de instrucciones en lenguaje natural.
- **Validación de joins y proyecciones** en queries generadas.
- **Logs de resultados** → `logs/`.
- **Slides de resultados** → generados manualmente en `slides/` (si aplica).

---

## 📌 Roadmap
- [x] Semana 2 → Estructura de datos + EDA + Logging
- [x] Semana 3 → Agente generador de queries + Validación avanzada
- [x] Semana 6 → Experimentos y validación automatizada
- [x] Semana 7 → Métricas avanzadas y análisis de aprendizaje

---

# Avances Sprint 2: Agente NL→MongoDB y EDA

## Mejoras implementadas
- Soporte para instrucciones complejas: desanidar, substring, concatenaciones, joins y agregaciones avanzadas.
- Feature engineering: generación automática de campos derivados (fechas formateadas, partes enteras/decimales, campo complejo `reg`).
- Validación y normalización de campos y operadores, usando sinónimos y rutas anidadas.
- Fallback inteligente: integración de modelos LLM (Azure OpenAI 4.1 y OpenRouter) para sugerencias y resolución de casos no cubiertos por reglas.
- Pruebas automáticas y validación de casos límite en notebooks.
- Visualizaciones EDA: heatmaps, análisis de cobertura y resumen ejecutivo de hallazgos.
- Documentación de recomendaciones y conclusiones accionables para robustecer el agente.

## Ejemplo de queries generadas
- Filtrado avanzado:
  ```json
  { "$match": { "Devices.ServicePoints.ShipOutCycles.Transactions.Total": { "$gt": 3000.0 } } }
  ```
- Join automático:
  ```json
  { "$lookup": { "from": "departamentos", "localField": "departamento_id", "foreignField": "departamento_id", "as": "departamentos_info" } }
  { "$unwind": "$departamentos_info" }
  ```
- Agregación temporal:
  ```json
  { "$addFields": { "anio_mes": { "$substr": ["$Date", 0, 7] } } }
  { "$group": { "_id": "$anio_mes", "suma_total_ventas": { "$sum": "$total" } } }
  ```

## Próximos pasos
- Ampliar el dataset con más ejemplos de instrucciones minoritarias.
- Fortalecer la lógica de fallback y monitoreo de drift.
- Documentar y analizar casos de error para mejorar la cobertura.

---

## 🧪 Experimentos y Validación (Sprint 2)

- Se añadió el notebook `EDA_Semana6.ipynb` con ejecución automática de experimentos y visualización comparativa de variantes del agente NL→MongoDB.
- Los experimentos evalúan el desempeño (accuracy y latencia) del baseline y variantes, mostrando resultados en tablas y gráficos.
- Todos los agentes y variantes alcanzan accuracy perfecto en los ejemplos evaluados, con latencia mínima.
- El flujo experimental permite comparar fácilmente mejoras y justificar la adopción de variantes.

**Ejemplo de visualización:**
- Gráfico de barras y líneas para comparar accuracy y latencia entre baseline, variantes y agente real.
- Tabla resumen con métricas principales.

## 📊 Análisis de Aprendizaje (Semana 7)

### Métricas Avanzadas
- Implementación de normalización de campos con manejo de acentos y casos
- Evaluación dinámica de campos esperados según tipo de consulta
- Métricas de recall mejoradas: de ~39% a ~67%

### Curvas de Aprendizaje
- Accuracy de entrenamiento: 100%
- Accuracy de prueba: mejora hasta 97%
- Estabilización del modelo: 60-70 ejemplos
- Visualización de curvas de aprendizaje para análisis de rendimiento

### Mejoras Implementadas
- Normalización de campos para comparaciones flexibles
- Expectativas dinámicas basadas en contexto de consulta
- Sistema de evaluación más realista y contextual
- Análisis detallado de curvas de aprendizaje

### Resultados Clave
- Alta precisión en conjunto de entrenamiento
- Buena generalización en pruebas (~97%)
- Identificación del punto óptimo de ejemplos (60+)
- Validación de robustez del modelo


# Semana 8: Optimización de Hiperparámetros y Análisis de Errores

Este repositorio contiene el cuaderno principal para los experimentos de la semana 8:

- **Cuaderno principal:** `notebooks/EDA_Semana8.ipynb`

## Descripción

En el cuaderno de la semana 8 se implementan y comparan dos enfoques de optimización de hiperparámetros para el agente generador de queries MongoDB:
- Búsqueda Aleatoria (Random Search)
- Optimización Bayesiana (Optuna)

Incluye:
- Pruning y early stopping
- Registro de logs y artefactos
- Tabla top-k y gráficos de evolución
- Métricas de cobertura, recall, precisión y F1
- Análisis de errores y recomendaciones

## Ejecución

1. Abre el archivo `notebooks/EDA_Semana8.ipynb` en Jupyter o VS Code.
2. Ejecuta las celdas en orden para reproducir los experimentos y visualizar los resultados.
3. Los resultados y artefactos se guardan automáticamente en la carpeta `results/`.

## Recomendaciones

- Revisa las métricas y gráficos para comparar configuraciones.
- Consulta las celdas de análisis de errores para identificar oportunidades de mejora en el agente.
- Puedes modificar las consultas y campos esperados para ampliar los experimentos.

---

Para dudas o mejoras, consulta la sección de recomendaciones al final del cuaderno.

# Semana 9: Evaluación, Cobertura y Optimización Final

En la semana 9 se realizó un análisis exhaustivo de la cobertura y precisión semántica del agente inteligente para queries MongoDB, utilizando un conjunto ampliado de ejemplos y reglas de matching contextual mejoradas.

## Descripción

- Se evaluó el agente con 50+ instrucciones variadas, midiendo la precisión semántica (proporción de campos esperados correctamente generados).
- Se implementaron visualizaciones para analizar aciertos y errores por instrucción, identificando patrones y casos difíciles.
- Se analizaron los errores semánticos, mostrando los campos generados vs. esperados para cada caso problemático.
- Se documentaron recomendaciones para mejorar la cobertura y robustez del agente.

## Principales Resultados

- Precisión semántica promedio superior al 90% en el conjunto de prueba.
- Identificación de instrucciones con menor cobertura, permitiendo focalizar mejoras.
- Visualización clara de aciertos y errores, facilitando el análisis de casos límite.
- Reglas de matching contextual y normalización mejoradas, reduciendo errores por variaciones en el lenguaje natural.

## Recomendaciones y Próximos Pasos

- Seguir ampliando el dataset con instrucciones minoritarias y casos reales.
- Refinar las reglas de matching y detección de operadores para instrucciones ambiguas.
- Implementar validación cruzada y pruebas con usuarios para robustecer la evaluación.
- Documentar y analizar sistemáticamente los errores para guiar mejoras iterativas.

Consulta el cuaderno `notebooks/Semana9.ipynb` para detalles, visualizaciones y análisis completos.