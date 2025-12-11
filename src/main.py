from fastapi import FastAPI, Body
from dotenv import load_dotenv
import os
load_dotenv()

# Prints de depuración para variables de entorno
print("API_KEY:", os.getenv("AZURE_OPENAI_API_KEY"))
print("ENDPOINT:", os.getenv("AZURE_OPENAI_ENDPOINT"))
print("DEPLOYMENT:", os.getenv("AZURE_OPENAI_DEPLOYMENT"))

from src.dataset_manager import DatasetManager
from src.AgenteGeneradorQueryMongo import SmartMongoQueryGenerator
import os
from src.llm_suggestion_engine import LLMSuggestionEngine
import json
from pydantic import BaseModel
import re

app = FastAPI(
    title="MongoDB Query Generator API",
    description="API unificada para generar queries MongoDB y sugerencias inteligentes con LLM",
    version="2.0.0"
)

dataset_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datasets/'))
manager = DatasetManager(dataset_path=dataset_path)
generator = SmartMongoQueryGenerator(dataset_manager=manager)
llm_engine = LLMSuggestionEngine()

class QueryRequest(BaseModel):
    natural_text: str

def is_valid_mongo_field(field):
    # MongoDB permite muchos caracteres, pero aquí restringimos a alfanuméricos y guion bajo
    return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', field) is not None

def validate_project_fields(stage):
    if "$project" in stage:
        # Obtener campos válidos del esquema de la colección si está disponible
        # Se asume que la variable 'generator' y su dataset_manager están accesibles
        # Si no se puede determinar la colección, solo validar sintaxis del nombre
        try:
            # Buscar la colección activa (solo para endpoint /assist/)
            from fastapi import Request
            import inspect
            frame = inspect.currentframe()
            while frame:
                if 'collection' in frame.f_locals:
                    collection = frame.f_locals['collection']
                    break
                frame = frame.f_back
            else:
                collection = None
        except Exception:
            collection = None

        schema_fields = set()
        if collection and hasattr(generator, 'dataset_manager') and generator.dataset_manager and hasattr(generator.dataset_manager, 'schemas') and collection in generator.dataset_manager.schemas:
            schema_obj = generator.dataset_manager.schemas[collection]
            if hasattr(schema_obj, 'fields'):
                # Puede ser dict o lista
                if isinstance(schema_obj.fields, dict):
                    schema_fields = set(schema_obj.fields.keys())
                else:
                    schema_fields = set(f.name if hasattr(f, 'name') else f for f in schema_obj.fields)

        # Detectar campos generados por $group en cualquier parte previa o actual del pipeline
        generated_fields = set()
        import inspect
        frame = inspect.currentframe()
        pipeline = None
        while frame:
            if 'pipeline' in frame.f_locals:
                pipeline = frame.f_locals['pipeline']
                break
            frame = frame.f_back
        if pipeline:
            for st in pipeline:
                if "$group" in st:
                    generated_fields.update(st["$group"].keys())
        for field in stage["$project"]:
            # Permitir '_id' como campo válido aunque no esté en el esquema
            if field == '_id':
                continue
            # Permitir campos generados por $group (como 'num_compras')
            if field in generated_fields:
                continue
            # Permitir campos que son asignaciones directas a campos generados por $group (ej: cliente_id: '$_id')
            if isinstance(stage["$project"][field], str) and stage["$project"][field].startswith("$"):
                ref_field = stage["$project"][field][1:]
                if ref_field in generated_fields or ref_field == '_id':
                    continue
            # Si hay esquema, solo validar que el campo esté en el esquema
            if schema_fields:
                if field not in schema_fields:
                    return False, f"El campo '{field}' en $project no existe en el esquema de la colección '{collection}'."
            else:
                # Si no hay esquema, solo validar sintaxis
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
    # Recolectar todos los campos generados por $group antes de validar $project
    all_generated_fields = set()
    for stage in pipeline:
        if "$group" in stage:
            all_generated_fields.update(stage["$group"].keys())
    for i, stage in enumerate(pipeline):
        if not isinstance(stage, dict):
            return False, f"El stage {i} no es un diccionario."
        for op in stage:
            if not op.startswith("$"):
                return False, f"El operador '{op}' en el stage {i} no empieza con '$'."
            if op not in VALID_OPERATORS:
                return False, f"El operador '{op}' en el stage {i} no es válido."
        # Validar campos de $project, pasando los campos generados globalmente
        if "$project" in stage:
            def validate_project_fields_with_generated(stage, generated_fields):
                if "$project" in stage:
                    # ...existing code...
                    try:
                        from fastapi import Request
                        import inspect
                        frame = inspect.currentframe()
                        while frame:
                            if 'collection' in frame.f_locals:
                                collection = frame.f_locals['collection']
                                break
                            frame = frame.f_back
                        else:
                            collection = None
                    except Exception:
                        collection = None
                    schema_fields = set()
                    if collection and hasattr(generator, 'dataset_manager') and generator.dataset_manager and hasattr(generator.dataset_manager, 'schemas') and collection in generator.dataset_manager.schemas:
                        schema_obj = generator.dataset_manager.schemas[collection]
                        if hasattr(schema_obj, 'fields'):
                            if isinstance(schema_obj.fields, dict):
                                schema_fields = set(schema_obj.fields.keys())
                            else:
                                schema_fields = set(f.name if hasattr(f, 'name') else f for f in schema_obj.fields)
                    for field in stage["$project"]:
                        if field == '_id':
                            continue
                        if field in generated_fields:
                            continue
                        if isinstance(stage["$project"][field], str) and stage["$project"][field].startswith("$"):
                            ref_field = stage["$project"][field][1:]
                            if ref_field in generated_fields or ref_field == '_id':
                                continue
                        if schema_fields:
                            if field not in schema_fields:
                                return False, f"El campo '{field}' en $project no existe en el esquema de la colección '{collection}'."
                        else:
                            if not is_valid_mongo_field(field):
                                return False, f"El campo '{field}' en $project no es un nombre válido de campo MongoDB."
                return True, None
            is_valid, error_msg = validate_project_fields_with_generated(stage, all_generated_fields)
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
        # Inferir la colección a partir de la instrucción usando heurísticas y sinónimos
        def infer_collection(text):
            text_lower = text.lower()
            collection_candidates = list(generator.dataset_manager.schemas.keys())
            synonyms = {
                'ventas': ['venta', 'ventas', 'total_venta', 'total_ventas', 'importe', 'monto', 'más vendidos', 'mas vendidos', 'vendidos', 'top ventas', 'top vendidos', 'producto más vendido', 'productos más vendidos', 'producto mas vendido', 'productos mas vendidos', 'compraron', 'veces', 'compra', 'compras'],
                'clientes': ['cliente', 'clientes', 'usuario', 'comprador', 'user', 'customer', 'client'],
                'productos': ['producto', 'productos', 'articulo', 'item', 'mercancia'],
                'empleados': ['empleado', 'empleados', 'personal', 'colaborador', 'worker', 'staff'],
                'usuarios': ['usuario', 'usuarios', 'user', 'users'],
                'transactions_collection': ['transaccion', 'transacciones', 'movimiento', 'transactions', 'transacción', 'transacciones', 'transacciones mayores', 'monto transaccion', 'muestra las transacciones'],
            }

            # Nueva regla: Si la consulta menciona 'precio promedio' y 'producto(s)', priorizar 'productos' si existe
            if (('precio promedio' in text_lower or 'promedio de precio' in text_lower or 'precio medio' in text_lower) and ('producto' in text_lower or 'productos' in text_lower)):
                if 'productos' in collection_candidates:
                    return 'productos'

            # Regla especial: si la consulta menciona clientes y compras/veces, priorizar ventas
            if (('cliente' in text_lower or 'clientes' in text_lower) and ('compraron' in text_lower or 'compra' in text_lower or 'compras' in text_lower or 'veces' in text_lower)):
                if 'ventas' in collection_candidates:
                    return 'ventas'
            # Prioridad especial para ventas si la instrucción es sobre productos vendidos
            ventas_phrases = ['más vendidos', 'mas vendidos', 'top ventas', 'top vendidos', 'producto más vendido', 'productos más vendidos', 'producto mas vendido', 'productos mas vendidos', 'vendidos']
            if any(phrase in text_lower for phrase in ventas_phrases):
                if 'ventas' in collection_candidates:
                    return 'ventas'
            # Buscar por nombre directo
            for coll in collection_candidates:
                if coll in text_lower:
                    return coll
            # Buscar por sinónimos
            for coll, syns in synonyms.items():
                for syn in syns:
                    if syn in text_lower:
                        return coll
            # Fallback: primera colección
            return collection_candidates[0] if collection_candidates else "labs"

        collection = infer_collection(natural_text)
        print("Colección inferida final jaja:", collection)
        # Generar el pipeline como objeto Python usando la colección inferida
        # pipeline = generator.generate_query(natural_text, collection=collection)
        # is_valid, error_msg = validate_pipeline_structure(pipeline)
        # if not is_valid:
        #     return {
        #         "error": f"Pipeline inválido: {error_msg}",
        #         "status": "error"
        #     }
        # print("Colección inferida:", collection)
        # Generar la query como string usando el método correcto y la colección inferida
        query_str = generator.generate_query(collection, natural_text)
        suggestions = llm_engine.suggest_query_improvement(natural_text, query_str)
        # Si la query contiene 'campo_no_encontrado', sugerir los campos válidos de la colección
        extra_suggestion = None
        if isinstance(query_str, str) and 'campo_no_encontrado' in query_str:
            # Obtener campos válidos de la colección
            campos_validos = []
            if hasattr(generator, 'dataset_manager') and generator.dataset_manager and hasattr(generator.dataset_manager, 'schemas') and collection in generator.dataset_manager.schemas:
                schema_obj = generator.dataset_manager.schemas[collection]
                if hasattr(schema_obj, 'fields'):
                    if isinstance(schema_obj.fields, dict):
                        campos_validos = list(schema_obj.fields.keys())
                    else:
                        campos_validos = [f.name if hasattr(f, 'name') else f for f in schema_obj.fields]
            if campos_validos:
                extra_suggestion = f"Campos válidos en la colección '{collection}': {', '.join(campos_validos)}"
        return {
            "query": query_str,
            "suggestions": (suggestions["suggestions"] if not extra_suggestion else [extra_suggestion]),
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