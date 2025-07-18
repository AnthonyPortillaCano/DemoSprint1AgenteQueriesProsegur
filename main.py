from fastapi import FastAPI, Body
from AgenteGeneradorQueryMongo import SmartMongoQueryGenerator
from llm_suggestion_engine import LLMSuggestionEngine

app = FastAPI(
    title="MongoDB Query Generator API",
    description="API para generar queries MongoDB desde lenguaje natural con sugerencias LLM",
    version="1.0.0"
)

# Inicializar componentes
generator = SmartMongoQueryGenerator()
llm_engine = LLMSuggestionEngine()

@app.post("/generate_query/")
def generate_query(collection: str = Body(...), natural_text: str = Body(...)):
    """
    🎯 GENERAR QUERY MONGODB
    
    Genera una query MongoDB desde texto en lenguaje natural.
    Solo genera la query, sin sugerencias automáticas para control de costos.
    """
    try:
        query = generator.generate_query(collection, natural_text)
        return {
            "query": query,
            "status": "success",
            "suggestions": None  # Sin sugerencias automáticas
        }
    except Exception as e:
        return {
            "error": str(e),
            "status": "error",
            "suggestions": None
        }

@app.post("/get_suggestions/")
def get_suggestions(natural_text: str = Body(...), current_result: str = Body(None)):
    """
    🧠 OBTENER SUGERENCIAS LLM
    
    Genera sugerencias inteligentes para mejorar una query.
    Solo se ejecuta cuando el usuario lo solicita explícitamente.
    """
    result = llm_engine.suggest_query_improvement(natural_text, current_result)
    return result

@app.post("/suggest_field_mapping/")
def suggest_field_mapping(unknown_field: str = Body(...), available_fields: list = Body(...)):
    """
    🎯 SUGERENCIAS DE MAPEO DE CAMPOS
    
    Sugiere campos disponibles cuando se detecta un campo desconocido.
    """
    result = llm_engine.suggest_field_mapping(unknown_field, available_fields)
    return result

@app.get("/health/")
def health_check():
    """
    🏥 VERIFICACIÓN DE SALUD
    
    Verifica el estado de la API y componentes.
    """
    llm_stats = llm_engine.get_usage_stats()
    return {
        "status": "healthy",
        "llm_available": llm_stats["llm_available"],
        "model": llm_stats["model"],
        "version": "1.0.0"
    } 