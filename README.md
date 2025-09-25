
# Agente Inteligente para la Generación Automática de Queries MongoDB a partir de Lenguaje Natural

## Estructura del repositorio

```
DemoSprint1Final/
├── datasets/          # Dataset usado por el agente
├── notebooks/         # Jupyter Notebooks (ejemplo principal: AgenteInteligente_QueriesMongoDB.ipynb)
├── src/               # Código fuente principal
├── requirements.txt   # Dependencias del proyecto
├── README.md          # Este archivo
└── ...otros archivos
```



## Requisitos previos

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

## Instalación de dependencias y entorno virtual

Se recomienda crear un entorno virtual para aislar las dependencias:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

En Linux/MacOS:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ejecución del pipeline



## Ejecución del pipeline

Para iniciar el API (FastAPI):
```powershell
uvicorn src.main:app --reload
```

Para iniciar la web (Streamlit):
```powershell
streamlit run src/app.py
```
## Variables de entorno

Si usas claves API u otras variables sensibles, crea un archivo `.env` en la raíz del proyecto con el siguiente formato:

```
AZURE_OPENAI_API_KEY=tu_clave
AZURE_OPENAI_ENDPOINT=tu_endpoint
AZURE_OPENAI_DEPLOYMENT=tu_deployment
```

Consulta el README o los comentarios del código para más detalles sobre variables requeridas.

También puedes abrir el notebook principal en la carpeta `notebooks/` y seguir los ejemplos para generar queries MongoDB desde lenguaje natural.

## Dataset documentado

- **Fuente:** Prosegur, exportación interna de sistema de transacciones
- **Fecha de descarga:** 2025-09-01
- **Tamaño:** 7kb
- **Variables principales:** deviceId, branchCode, subChannelCode, shipOutCode, currencyCode, confirmationCode, date, total, departamento_id, departamento_nombre
- **Hash:** Ejecuta este comando para obtener el hash del archivo principal:
   ```bash
   certutil -hashfile datasets/transactions_collection.json SHA256
   ```

## Recomendaciones
- Mantén los scripts principales en `src/` para mayor orden.
- Documenta cualquier notebook en la carpeta `notebooks/`.
- Actualiza este README con instrucciones específicas de tu flujo.

---

## Roadmap Sprint 1

- **Objetivo:** Desarrollar un agente capaz de generar queries MongoDB a partir de instrucciones en lenguaje natural, incluyendo soporte para joins dinámicos.
- **Tareas principales:**
   - Definir y documentar el dataset.
   - Implementar el parser de lenguaje natural.
   - Desarrollar el generador de queries MongoDB.
   - Crear notebook demostrativo reproducible.
   - Mejorar la detección de sinónimos y variantes lingüísticas.
   - Validar el pipeline con ejemplos reales.
- **Avances:**
   - Estructura de repositorio ordenada y funcional.
   - Dataset cargado y documentado.
   - Pipeline reproducible en notebook principal.
   - Código modular en carpeta `src/`.

## Plan de recuperación

- **Backup del dataset:** Mantener copia original en `datasets/` y documentar hash SHA256.
- **Manejo de errores:** El agente valida campos y colecciones, mostrando sugerencias si hay errores en la instrucción.
- **Reproducibilidad:** El notebook principal puede ejecutarse desde cero en cualquier entorno con las dependencias instaladas.
- **Control de versiones:** El repositorio está sincronizado con GitHub para recuperación ante fallos.