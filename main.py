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

app = FastAPI(
    title="MongoDB Query Generator API",
    description="API unificada para generar queries MongoDB y sugerencias inteligentes con LLM",
    version="2.0.0"
)

generator = SmartMongoQueryGenerator()
llm_engine = LLMSuggestionEngine()

@app.post("/assist/")
def assist(
    collection: str = Body(..., description="Nombre de la colección MongoDB"),
    natural_text: str = Body(..., description="Instrucción en lenguaje natural")
):
    """
    Endpoint único que genera la query y sugiere mejoras o ayuda usando LLM.
    """
    try:
        query = generator.generate_query(collection, natural_text)
        suggestions = llm_engine.suggest_query_improvement(natural_text, query)
        return {
            "query": query,
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