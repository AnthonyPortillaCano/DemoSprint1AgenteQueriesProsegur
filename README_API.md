# 🎯 MongoDB Query Generator API

API REST para generar queries MongoDB desde lenguaje natural con sugerencias LLM usando Azure OpenAI.

## 🚀 Características

- ✅ **Generación de queries MongoDB** desde lenguaje natural
- 🧠 **Sugerencias LLM** con Azure OpenAI (control de costos)
- 📊 **Validación de campos** y mapeo inteligente
- 🏥 **Health check** y monitoreo
- 📚 **Documentación automática** (Swagger UI)

## 🛠️ Instalación

### 1. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar Azure OpenAI
Crea un archivo `.env` basado en `env_example.txt`:

```bash
# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY=your_azure_openai_api_key_here
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
```

### 3. Ejecutar la API
```bash
# Desarrollo (con recarga automática)
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Producción
uvicorn main:app --host 0.0.0.0 --port 8000
```

## 📖 Uso de la API

### Documentación Interactiva
Accede a [http://localhost:8000/docs](http://localhost:8000/docs) para la documentación Swagger UI.

### Endpoints Principales

#### 1. Generar Query MongoDB
```bash
POST /generate_query/
```
```json
{
  "collection": "transactions",
  "natural_text": "desanidar todos los niveles de devices hasta transactions\nagrupar por fecha, deviceId\nproyectar campo reg concatenando los valores según la plantilla\nordenar por fecha"
}
```

#### 2. Obtener Sugerencias LLM
```bash
POST /get_suggestions/
```
```json
{
  "natural_text": "desanidar devices y agrupar por fecha",
  "current_result": "db.getCollection(\"transactions\").aggregate([...])"
}
```

#### 3. Sugerencias de Mapeo de Campos
```bash
POST /suggest_field_mapping/
```
```json
{
  "unknown_field": "dispositivo",
  "available_fields": ["deviceId", "branchCode", "subChannelCode"]
}
```

#### 4. Health Check
```bash
GET /health/
```

## 🐳 Docker

### Construir imagen
```bash
docker build -t mongoquery-api .
```

### Ejecutar contenedor
```bash
docker run -p 8000:8000 --env-file .env mongoquery-api
```

## 💰 Control de Costos

La API está diseñada para **control de costos**:

- ✅ **Sin sugerencias automáticas** - Solo cuando se solicitan
- 🎯 **Modelo económico** - GPT-35-turbo
- 📊 **Límites de tokens** - Max 300 tokens por sugerencia
- 💡 **Costo estimado** - Incluido en cada respuesta

### Costos Estimados (GPT-35-turbo)
- **Input:** $0.0015 por 1K tokens
- **Output:** $0.002 por 1K tokens
- **Sugerencia típica:** ~$0.001-0.002 por consulta

## 🔧 Configuración Avanzada

### Variables de Entorno
| Variable | Descripción | Requerido |
|----------|-------------|-----------|
| `AZURE_OPENAI_API_KEY` | API key de Azure OpenAI | ✅ |
| `AZURE_OPENAI_ENDPOINT` | Endpoint de Azure OpenAI | ✅ |
| `AZURE_OPENAI_MODEL` | Modelo a usar (default: gpt-35-turbo) | ❌ |

### Sin Azure OpenAI
Si no configuras Azure OpenAI, la API funcionará sin sugerencias LLM:
- ✅ Generación de queries MongoDB
- ❌ Sugerencias inteligentes
- ⚠️ Mensaje de advertencia en logs

## 📊 Monitoreo

### Health Check Response
```json
{
  "status": "healthy",
  "llm_available": true,
  "model": "gpt-35-turbo",
  "version": "1.0.0"
}
```

### Logs de la API
- Configuración de Azure OpenAI
- Errores de generación de queries
- Uso de LLM y costos estimados

## 🚀 Despliegue en Azure

### Azure Container Registry
```bash
# Login a ACR
az acr login --name <tu-acr>

# Tag y push
docker tag mongoquery-api <tu-acr>.azurecr.io/mongoquery-api:latest
docker push <tu-acr>.azurecr.io/mongoquery-api:latest
```

### Azure Container Instances
```bash
az container create \
  --resource-group <tu-rg> \
  --name mongoquery-api \
  --image <tu-acr>.azurecr.io/mongoquery-api:latest \
  --ports 8000 \
  --environment-variables AZURE_OPENAI_API_KEY=<tu-key> AZURE_OPENAI_ENDPOINT=<tu-endpoint>
```

## 🐛 Troubleshooting

### Error: "LLM no disponible"
- Verificar configuración de Azure OpenAI
- Revisar variables de entorno
- Comprobar conectividad de red

### Error: "JSON decode error"
- Usar `\n` para saltos de línea en JSON
- Verificar formato del JSON
- Usar Swagger UI para testing

### Error: "Module not found"
- Instalar dependencias: `pip install -r requirements.txt`
- Verificar estructura de archivos
- Comprobar imports en `main.py` 