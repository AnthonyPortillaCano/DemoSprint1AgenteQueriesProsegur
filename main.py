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
from pydantic import BaseModel
import re

app = FastAPI(
    title="MongoDB Query Generator API",
    description="API unificada para generar queries MongoDB y sugerencias inteligentes con LLM",
    version="2.0.0"
)

generator = SmartMongoQueryGenerator()
llm_engine = LLMSuggestionEngine()

class QueryRequest(BaseModel):
    natural_text: str

def is_valid_mongo_field(field):
    # MongoDB permite muchos caracteres, pero aquí restringimos a alfanuméricos y guion bajo
    return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', field) is not None

def validate_project_fields(stage):
    if "$project" in stage:
        for field in stage["$project"]:
            if not is_valid_mongo_field(field):
                return False, f"El campo '{field}' en $project no es un nombre válido de campo MongoDB."
    return True, None

def validate_pipeline_structure(pipeline):
    """
    Valida que el pipeline tenga la estructura básica de un pipeline de MongoDB.
    Retorna (True, None) si es válido, (False, mensaje) si no.
    """
    VALID_OPERATORS = {
        "$match", "$group", "$project", "$unwind", "$sort", "$limit", "$skip",
        "$lookup", "$addFields", "$set", "$unset", "$replaceRoot", "$count",
        "$facet", "$bucket", "$bucketAuto", "$sortByCount", "$sample"
    }
    if not isinstance(pipeline, list):
        return False, "El pipeline no es una lista."
    for i, stage in enumerate(pipeline):
        if not isinstance(stage, dict):
            return False, f"El stage {i} no es un diccionario."
        for op in stage:
            if not op.startswith("$"):
                return False, f"El operador '{op}' en el stage {i} no empieza con '$'."
            if op not in VALID_OPERATORS:
                return False, f"El operador '{op}' en el stage {i} no es válido."
        # Validar campos de $project
        is_valid, error_msg = validate_project_fields(stage)
        if not is_valid:
            return False, error_msg
    return True, None

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
def assist(request: QueryRequest):
    natural_text = request.natural_text
    try:
        # Usar 'labs' como nombre fijo de la colección
        collection = "labs"
        # Generar el pipeline como objeto Python
        pipeline = generator.parse_natural_language(natural_text)
        is_valid, error_msg = validate_pipeline_structure(pipeline)
        if not is_valid:
            return {
                "error": f"Pipeline inválido: {error_msg}",
                "status": "error"
            }
        # Generar la query como string usando el método correcto
        query_str = generator.generate_query(collection, natural_text)
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