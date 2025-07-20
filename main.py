from fastapi import FastAPI, Body
from dotenv import load_dotenv
import os
load_dotenv()

# Prints de depuración para variables de entorno
print("API_KEY:", os.getenv("AZURE_OPENAI_API_KEY"))
print("ENDPOINT:", os.getenv("AZURE_OPENAI_ENDPOINT"))
print("DEPLOYMENT:", os.getenv("AZURE_OPENAI_DEPLOYMENT"))

from AgenteGeneradorQueryMongo import SmartMongoQueryGenerator
from llm_suggestion_engine import LLMSuggestionEngine
import json

app = FastAPI(
    title="MongoDB Query Generator API",
    description="API unificada para generar queries MongoDB y sugerencias inteligentes con LLM",
    version="2.0.0"
)

generator = SmartMongoQueryGenerator()
llm_engine = LLMSuggestionEngine()

def format_query_for_mongodb(collection: str, pipeline: list) -> str:
    """
    Formatea la query para MongoDB sin caracteres de escape,
    lista para copiar y pegar en editores como NoSQLBooster.
    """
    # Convertir el pipeline a JSON sin escape de comillas
    pipeline_json = json.dumps(pipeline, indent=2, separators=(',', ': '))
    # Reemplazar las comillas escapadas por comillas normales
    pipeline_json = pipeline_json.replace('\\"', '"')
    # Crear la query completa
    query = f'db.getCollection("{collection}").aggregate({pipeline_json})'
    return query

@app.post("/assist/")
def assist(
    collection: str = Body(..., description="Nombre de la colección MongoDB"),
    natural_text: str = Body(..., description="Instrucción en lenguaje natural")
):
    """
    Endpoint único que genera la query y sugiere mejoras o ayuda usando LLM.
    Devuelve la query lista para pegar en editores MongoDB sin caracteres de escape.
    """
    try:
        # Generar el pipeline como objeto Python
        pipeline = generator.generate_pipeline(collection, natural_text)
        # Formatear la query para MongoDB (sin caracteres de escape)
        query_str = format_query_for_mongodb(collection, pipeline)
        suggestions = llm_engine.suggest_query_improvement(natural_text, query_str)
        return {
            "query": query_str,
            "suggestions": suggestions["suggestions"],
            "status": "success"
        }
    except Exception as e:
        suggestions = llm_engine.suggest_query_improvement(natural_text, str(e))
        return {
            "error": str(e),
            "suggestions": suggestions["suggestions"],
            "status": "error"
        }

@app.get("/health/")
def health_check():
    llm_stats = llm_engine.get_usage_stats()
    return {
        "status": "healthy",
        "llm_available": llm_stats["llm_available"],
        "model": llm_stats["model"],
        "version": "2.0.0"
    } 