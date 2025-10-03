

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
 ├── EDA_Semana3.ipynb              # Análisis exploratorio y validación de datos
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
   - Abrir y ejecutar el notebook `notebooks/EDA_Semana3.ipynb` para explorar los datos y verificar ejemplos.

2. **Probar agente generador de queries**
   - Abrir y ejecutar el notebook `notebooks/AgenteInteligente_QueriesMongoDB.ipynb`.
   - Modificar la variable `instruccion` para probar diferentes frases en lenguaje natural.
   - Validar que el agente genera queries correctas, incluyendo instrucciones con joins y proyecciones dinámicas.

3. **Ejecutar scripts principales**
   - Ejecutar `src/main.py` para pruebas integradas.
   - Para iniciar el API (FastAPI):
  ```powershell
     uvicorn src.main:app --reload
   - Para iniciar la web (Streamlit):
    ```powershell
    streamlit run src/app.py
---

## Variables de entorno

Si usas claves API u otras variables sensibles, crea un archivo `.env` en la raíz del proyecto con el siguiente formato:

```
AZURE_OPENAI_API_KEY=tu_clave
AZURE_OPENAI_ENDPOINT=tu_endpoint
AZURE_OPENAI_DEPLOYMENT=tu_deployment
```

Consulta el README o los comentarios del código para más detalles sobre variables requeridas.

También puedes abrir el notebook principal en la carpeta `notebooks/` y seguir los ejemplos para generar queries MongoDB desde lenguaje natural.


## 📈 Resultados esperados (Semana 3)
- **EDA inicial** en `notebooks/EDA_Semana3.ipynb`.
- **Agente genera queries MongoDB** a partir de instrucciones en lenguaje natural.
- **Validación de joins y proyecciones** en queries generadas.
- **Logs de resultados** → `logs/`.
- **Slides de resultados** → generados manualmente en `slides/` (si aplica).

---

## 📌 Roadmap
- [x] Semana 2 → Estructura de datos + EDA + Logging
- [x] Semana 3 → Agente generador de queries + Validación avanzada