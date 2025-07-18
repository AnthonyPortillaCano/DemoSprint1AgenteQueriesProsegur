# 🎯 MongoDB Query Generator API

API REST unificada para generar queries MongoDB desde lenguaje natural y recibir sugerencias inteligentes usando LLM (Azure OpenAI).

## 🚀 Características

- ✅ **Un solo endpoint inteligente** (`/assist/`)
- 🧠 **Sugerencias LLM** con Azure OpenAI
- 📊 **Validación y ayuda automática**
- 🏥 **Health check**
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
AZURE_OPENAI_DEPLOYMENT=gpt-35-turbo
```

### 3. Ejecutar la API
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## 📖 Uso de la API

### Documentación Interactiva
Accede a [http://localhost:8000/docs](http://localhost:8000/docs) para la documentación Swagger UI.

### Endpoint Único

#### `/assist/` (POST)
Genera una query MongoDB y sugiere mejoras o ayuda usando LLM.

**Ejemplo de request:**
```json
{
  "collection": "labs",
  "natural_text": "desanidar todos los niveles de devices hasta transactions, agrupar por fecha, deviceId, proyectar campo reg concatenando los valores según la plantilla, ordenar por fecha"
}
```

**Ejemplo de respuesta exitosa:**
```json
{
  "query": "db.labs.aggregate([...])",
  "suggestions": "Considera especificar el formato de la fecha para mayor claridad...",
  "status": "success"
}
```

**Ejemplo de respuesta con error:**
```json
{
  "error": "Campo 'deviceId' no encontrado en la colección 'labs'...",
  "suggestions": "¿Quizás quisiste decir 'device_id'? Revisa los campos disponibles...",
  "status": "error"
}
```

### Health Check
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
- Sugerencias LLM solo cuando es necesario
- Modelo económico (GPT-35-turbo)
- Límite de tokens por respuesta

## 🔧 Configuración Avanzada

### Variables de Entorno
| Variable | Descripción | Requerido |
|----------|-------------|-----------|
| `AZURE_OPENAI_API_KEY` | API key de Azure OpenAI | ✅ |
| `AZURE_OPENAI_ENDPOINT` | Endpoint de Azure OpenAI | ✅ |
| `AZURE_OPENAI_DEPLOYMENT` | Nombre del deployment/modelo en Azure | ✅ |

## 🚀 Despliegue en Azure

Sigue los pasos de la sección anterior para App Service o usa el workflow de GitHub Actions.

## 🐛 Troubleshooting
- Si `/assist/` responde con error, revisa los logs y la configuración de variables de entorno.
- Si no ves `/docs`, revisa el comando de inicio y el nombre del archivo principal.

## 📊 Monitoreo
- Usa `/health/` para verificar el estado de la API y la conexión con LLM. 