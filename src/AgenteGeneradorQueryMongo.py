
class AgenteGeneradorQueryMongo:
    @staticmethod
    def normaliza_campo_robusto(campo):
        import unicodedata, re
        campo = unicodedata.normalize('NFKD', campo).encode('ASCII', 'ignore').decode('utf-8').lower()
        campo = re.sub(r'[^a-z0-9]', '', campo)
        if len(campo) > 3 and campo.endswith('s'):
            campo = campo[:-1]
        return campo
import re
import json
from typing import Dict, List, Optional, Any, Union
try:
    from dataset_manager import DatasetManager, create_default_dataset
    from llm_suggestion_engine import LLMSuggestionEngine
except ImportError:
    from src.dataset_manager import DatasetManager, create_default_dataset
    from src.llm_suggestion_engine import LLMSuggestionEngine

class SmartMongoQueryGenerator:
    @staticmethod
    def normaliza_campo_robusto(campo):
        import unicodedata, re
        campo = unicodedata.normalize('NFKD', campo).encode('ASCII', 'ignore').decode('utf-8').lower()
        campo = re.sub(r'[^a-z0-9]', '', campo)
        if len(campo) > 3 and campo.endswith('s'):
            campo = campo[:-1]
        return campo
    def _filtrar_project_global(self, pipeline, schema_fields):
        for i, stage in enumerate(pipeline):
            if "$project" in stage:
                # Detectar campos generados por $group en el pipeline anterior
                generated_fields = set()
                if i > 0 and "$group" in pipeline[i-1]:
                    generated_fields.update(pipeline[i-1]["$group"].keys())
                # Permitir campos del esquema y generados por $group
                keys_a_borrar = [k for k in stage["$project"].keys() if k not in schema_fields and k not in generated_fields]
                for k in keys_a_borrar:
                    del stage["$project"][k]
    def _normalize_collection(self, collection: str) -> str:
        """Normaliza el nombre de la colección usando sinónimos y heurísticas."""
        if not collection:
            return collection
        collection_norm = collection.lower().replace(' ', '').replace('_', '')
        # Diccionario de sinónimos de colecciones
        collection_synonyms = {
            'transacciones': 'transactions_collection',
            'transactions': 'transactions_collection',
            'transaction': 'transactions_collection',
            'movimientos': 'transactions_collection',
            # Agrega más sinónimos si es necesario
        }
        for syn, canonical in collection_synonyms.items():
            if collection_norm == syn:
                return canonical
        # Si no hay coincidencia, retorna el original
        return collection
    def __init__(self, dataset_manager: Optional[DatasetManager] = None, llm_engine: Optional[LLMSuggestionEngine] = None, threshold: float = 0.5, use_synonyms: bool = True, use_schema_resolver: bool = False):
        # GESTOR DE DATASET (Nuevo - Contexto de Datos)
        self.dataset_manager = dataset_manager or create_default_dataset()
        self.llm_engine = llm_engine or LLMSuggestionEngine()
        self.threshold = threshold  # Controla el umbral de coincidencia para campos
        self.use_synonyms = use_synonyms  # Controla si se usan sinónimos para normalizar campos
        # Opt-in: resolver de esquema (mapeo de tokens a campos en dataset_manager)
        self.use_schema_resolver = use_schema_resolver
        # 📅 Formatos de fecha soportados

        # (Eliminado: lógica de es_simple y palabras_avanzadas, solo debe estar en generate_query)
        self.date_formats = {
            'YYYYMMDD': '%Y%m%d',
            'DDMMYYYY': '%d%m%Y',
            'YYYYMMDDHHMMSS': '%Y%m%d%H%M%S'
        }
        # 🔧 Mapeo de operadores (Bridging the Gap - Mapeo de Operadores)
        self.operator_map = {
            'suma': '$sum', 'sum': '$sum',
            'promedio': '$avg', 'average': '$avg',
            'máximo': '$max', 'max': '$max',
            'mínimo': '$min', 'min': '$min'
        }
        # 🧠 Estado del pipeline (SmBoP - Acumulación Progresiva)
        self.pipeline = []
        self.schema_cache = {}
        # 📚 SINÓNIMOS DE OPERACIONES (Bridging the Gap - Normalización de Texto)
        self.OPERATION_SYNONYMS = {
            'unwind': ['desanidar', 'unwind', 'expandir'],
            'group': ['agrupar', 'group', 'agrupar por'],
            'project': ['proyectar', 'project', 'seleccionar', 'mostrar'],
            'sort': ['ordenar', 'sort', 'ordenar por'],
            'sum': ['suma', 'sumar', 'sum'],
            'concat': ['concatenar', 'concat', 'unir'],
            'date': ['fecha', 'date', 'formato fecha'],
            'join': [
                'join', 'une', 'unes', 'unir', 'relaciona', 'relacionar',
                'combina', 'combinar', 'vincula', 'vincular', 'fusiona', 'fusionar'
            ]
        }
        # 🏷️ SINÓNIMOS DE CAMPOS (Bridging the Gap - Mapeo de Campos)
        self.FIELD_SYNONYMS = self._load_field_synonyms_from_dataset()

    def _load_field_synonyms_from_dataset(self):
        field_synonyms = {}
        # Cargar desde todas las colecciones del dataset
        for collection_name, schema in self.dataset_manager.schemas.items():
            for field_name, field_def in schema.fields.items():
                if field_name not in field_synonyms:
                    field_synonyms[field_name] = []
                # Agregar sinónimos del campo
                field_synonyms[field_name].extend(field_def.synonyms)
                # Agregar la ruta completa como sinónimo
                if field_def.path != field_name:
                    field_synonyms[field_name].append(field_def.path)
        # Fallback a sinónimos básicos si no hay dataset
        if not field_synonyms:
            # Diccionario generado automáticamente desde el EDA del notebook
            field_synonyms = {
                # --- INICIO AUTO-EXPANSIÓN AMPLIADA ---
                'empleado': ['empleado', 'empleados', 'personal', 'colaborador', 'worker', 'staff', 'trabajador', 'funcionario', 'emplead@', 'empl', 'empldo', 'empleadxs', 'emplead@s', 'emplead', 'empleadxs', 'emplead@s', 'empleadxs', 'emplead@s', 'empleadxs', 'emplead@s'],
                'departamento': ['departamento', 'departamentos', 'area', 'área', 'sector', 'division', 'división', 'seccion', 'sección', 'depart', 'dept', 'departament', 'departament@', 'departamentxs', 'departament@s', 'dpto', 'dptos', 'departament', 'departamentxs', 'departament@s'],
                'producto': ['producto', 'productos', 'articulo', 'artículo', 'item', 'art', 'prod', 'product', 'goods', 'mercancia', 'mercancía', 'mercaderia', 'mercadería', 'producto terminado', 'producto final', 'product@', 'productxs', 'product@s', 'total_ventas', 'total_venta', 'importe', 'monto'],
                'ventas': ['ventas', 'venta', 'total_ventas', 'vendidos', 'sales', 'vta', 'vtas', 'venta_total', 'ventas_totales', 'ventasnetas', 'ventas_brutas', 'ventas_netas', 'ventas_brutas', 'ventasrealizadas', 'ventasproyectadas', 'ventasestimadas', 'total_venta', 'total_ventas', 'importe', 'monto', 'ventas_totales', 'totalvendido', 'total_sold'],
                'nombre_cliente': ['nombre_cliente', 'cliente_nombre', 'nombre del cliente', 'nombrecliente', 'nombre-cliente', 'client_name', 'customer_name', 'nombre usuario', 'nombre comprador', 'nombrecliente', 'nombrecomprador', 'nombreusuario', 'denominación_cliente', 'denominacion_cliente', 'nombre', 'nombre del cliente', 'nombrecliente'],
                'total_venta': ['total_venta', 'importe', 'monto', 'total', 'valor_venta', 'venta_total', 'totalventa', 'total-venta', 'monto_total', 'importe_total', 'valor total', 'valorventa', 'totalfactura', 'totalfacturacion', 'totalfacturado', 'total_ventas', 'ventas', 'ventas_totales', 'totalvendido', 'total_sold'],
                'total_ventas': ['total_ventas', 'ventas_totales', 'ventas_total', 'totalventas', 'totalventas', 'ventasacumuladas', 'ventasglobales', 'totalventasnetas', 'totalventasbrutas', 'total_venta', 'ventas', 'importe', 'monto', 'suma_ventas', 'ventas_totales', 'totalvendido', 'total_sold'],
                'cliente': ['cliente', 'clientes', 'usuario', 'comprador', 'user', 'customer', 'client', 'comprad@r', 'client@', 'clientxs', 'client@s', 'compradorxs', 'comprador@s', 'compradorx', 'comprador@'],
                'compras': ['compras', 'adquisiciones', 'compra', 'purchases', 'purchase', 'adquisicion', 'adquisición', 'adq', 'adqs', 'comprasrealizadas', 'comprasnetas', 'comprasbrutas', 'comprasestimadas'],
                'empleados': ['empleados', 'empleado', 'personal', 'staff', 'colaboradores', 'trabajadores', 'funcionarios', 'empl', 'empldos', 'empleadxs', 'emplead@s'],
                'precio': ['precio', 'costo', 'valor', 'price', 'cost', 'valor_unitario', 'precio_unitario', 'preciofinal', 'precioventa', 'preciocompra', 'precio_bruto', 'precio_neto', 'precio estimado', 'precio sugerido', 'precio_promedio', 'precio_unitario', 'precio_total'],
                'ciudad': ['ciudad', 'localidad', 'municipio', 'city', 'poblacion', 'población', 'urbe', 'metropoli', 'metrópoli', 'ciudadela', 'villa', 'pueblo', 'capital', 'ciudadprincipal', 'ciudadsecundaria', 'ciudades', 'region'],
                'antiguedad': ['antiguedad', 'antigüedad', 'experiencia', 'años', 'seniority', 'tiempo', 'años_servicio', 'años_experiencia', 'añosantiguedad', 'añosantigüedad', 'añoslaborados', 'años_trabajados'],
                'stock': ['stock', 'existencia', 'inventario', 'existencias', 'almacen', 'almacén', 'disponible', 'disponibilidad', 'stockactual', 'stocktotal', 'stockminimo', 'stockmaximo', 'stockmínimo', 'stockmáximo', 'sin_stock', 'cantidad'],
                'area': ['area', 'área', 'zona', 'sector', 'region', 'región', 'espacio', 'área de trabajo', 'área funcional', 'área operativa', 'área administrativa', 'área comercial', 'área técnica', 'área producción'],
                'fecha_venta': ['fecha_venta', 'fecha', 'fecha de venta', 'fecha venta', 'fecha_transaccion', 'fechaoperacion', 'fechaventa', 'fechatransaccion', 'fechadeventa', 'fechadeoperacion', 'fecha_compra', 'momento', 'periodo', 'reciente'],
                'venta': ['venta', 'transaccion', 'transacción', 'operacion', 'operación', 'sale', 'transaction', 'venta_realizada', 'ventaestimada', 'ventabruta', 'ventaneta', 'ventatotal', 'total_ventas', 'total_venta', 'importe', 'monto'],
                'fecha': ['fecha', 'dia', 'día', 'mes', 'año', 'fecha_registro', 'fecha_creacion', 'fecha_modificacion', 'fecha_actualizacion', 'fecha_inicio', 'fecha_fin', 'fecha_cierre', 'fecha_apertura', 'fecha_emision', 'fecha_vencimiento', 'fecha_venta', 'fecha_compra', 'momento', 'periodo', 'reciente'],
                'frecuencia': ['frecuencia', 'habitualidad', 'repeticion', 'repetición', 'periodicidad', 'frecuencia_compra', 'frecuencia_venta', 'frecuenciauso', 'frecuenciavisita', 'frecuenciatransaccion', 'frecuenciacliente'],
                'descuento': ['descuento', 'rebaja', 'oferta', 'discount', 'descuentos', 'descuento_total', 'descuentototal', 'descuentoglobal', 'descuentounitario', 'descuentoespecial', 'descuentopromocional', 'descuentoadicional', 'promocion', 'bonificacion'],
                'proveedor': ['proveedor', 'suministrador', 'distribuidor', 'proveedores', 'supplier', 'vendor', 'proveedorasociado', 'proveedorprincipal', 'proveedorsecundario', 'proveedorlocal', 'proveedorexterno', 'proveedorinternacional', 'fabricante', 'vendedor'],
                'categoria': ['categoria', 'categoría', 'rubro', 'clase', 'tipo', 'segmento', 'grupo', 'familia', 'linea', 'línea', 'categoria_producto', 'categoria_servicio', 'categoria_cliente'],
                'denominación_cliente': ['denominación_cliente', 'nombre_cliente', 'cliente_nombre', 'nombre', 'nombre del cliente', 'nombrecliente'],
                'campo_no_encontrado': ['campo_no_encontrado', 'desconocido', 'missing_field', 'not_found'],
                # --- FIN AUTO-EXPANSIÓN AMPLIADA ---
            }
        return field_synonyms



    def _validate_field_with_dataset(self, field: str, collection_name: str = None) -> bool:
        if not self.dataset_manager:
            return True  # Sin dataset, asumir válido
        # Si se especifica colección, validar solo en esa
        if collection_name:
            return self.dataset_manager.validate_field(collection_name, field)
        # Validar en todas las colecciones
        for coll_name in self.dataset_manager.schemas.keys():
            if self.dataset_manager.validate_field(coll_name, field):
                return True
        return False

    def _suggest_fields_from_dataset(self, partial_name: str, collection_name: str = None) -> List[str]:
        suggestions = []
        if not self.dataset_manager:
            return suggestions
        # Si se especifica colección, buscar solo en esa
        if collection_name:
            suggestions = self.dataset_manager.suggest_fields(collection_name, partial_name)
        else:
            # Buscar en todas las colecciones
            for coll_name in self.dataset_manager.schemas.keys():
                suggestions.extend(self.dataset_manager.suggest_fields(coll_name, partial_name))
        return list(set(suggestions))  # Eliminar duplicados

    def _normalize_field(self, field: str, collection: str = None) -> str:
        import unicodedata, re
        from difflib import SequenceMatcher
        def norm(campo):
            campo = unicodedata.normalize('NFKD', campo).encode('ASCII', 'ignore').decode('utf-8').lower()
            campo = re.sub(r'[^a-z0-9]', '', campo)
            if len(campo) > 3 and campo.endswith('s'):
                campo = campo[:-1]
            return campo
        field_norm = norm(field)
        threshold = getattr(self, 'threshold', 0.8)
        use_synonyms = getattr(self, 'use_synonyms', True)
        # 1. Buscar en el dataset_manager la ruta real del campo (por path o sinónimos)
        if self.dataset_manager:
            collections = [collection] if collection else list(self.dataset_manager.schemas.keys())
            for coll in collections:
                schema = self.dataset_manager.schemas.get(coll)
                if not schema:
                    continue
                # Coincidencia exacta
                for fname, fdef in schema.fields.items():
                    if field_norm == norm(fname):
                        return fdef.path if fdef.path else fname
                # Coincidencia exacta en sinónimos (solo si use_synonyms)
                if use_synonyms:
                    for fname, fdef in schema.fields.items():
                        for syn in fdef.synonyms:
                            if field_norm == norm(syn):
                                return fdef.path if fdef.path else fname
                # Coincidencia por similitud (threshold) en nombres y sinónimos
                best_match = None
                best_ratio = 0
                for fname, fdef in schema.fields.items():
                    # Comparar con nombre
                    ratio = SequenceMatcher(None, field_norm, norm(fname)).ratio()
                    if ratio > best_ratio:
                        best_match = fdef.path if fdef.path else fname
                        best_ratio = ratio
                    # Comparar con sinónimos (solo si use_synonyms)
                    if use_synonyms:
                        for syn in fdef.synonyms:
                            ratio_syn = SequenceMatcher(None, field_norm, norm(syn)).ratio()
                            if ratio_syn > best_ratio:
                                best_match = fdef.path if fdef.path else fname
                                best_ratio = ratio_syn
                if best_match and best_ratio >= threshold:
                    return best_match
                # Si el threshold es muy bajo (<0.5), permite devolver el campo más parecido aunque no llegue al umbral
                if best_match and threshold < 0.5 and best_ratio > 0.4:
                    return best_match
        # 2. Fallback a FIELD_SYNONYMS (solo si use_synonyms)
        if use_synonyms:
            for canonical, synonyms in self.FIELD_SYNONYMS.items():
                if field_norm == norm(canonical):
                    return canonical
                for s in synonyms:
                    if field_norm == norm(s):
                        return canonical
        # 3. Coincidencia por similitud en sinónimos y nombres (threshold)
        best_match = None
        best_ratio = 0
        for canonical, synonyms in self.FIELD_SYNONYMS.items():
            # Comparar con nombre canónico
            ratio = SequenceMatcher(None, field_norm, norm(canonical)).ratio()
            if ratio > best_ratio:
                best_match = canonical
                best_ratio = ratio
            # Comparar con sinónimos (solo si use_synonyms)
            if use_synonyms:
                for s in synonyms:
                    ratio_syn = SequenceMatcher(None, field_norm, norm(s)).ratio()
                    if ratio_syn > best_ratio:
                        best_match = canonical
                        best_ratio = ratio_syn
        if best_match and best_ratio >= threshold:
            return best_match
        if best_match and threshold < 0.5 and best_ratio > 0.4:
            return best_match
        return field

    def _expand_special_phrases(self, field: str) -> list:

        # Traduce frases especiales a listas de campos reales
        if 'todos los niveles de devices hasta transactions' in field:
            return ["Devices", "Devices.ServicePoints", "Devices.ServicePoints.ShipOutCycles", "Devices.ServicePoints.ShipOutCycles.Transactions"]
        return [field]

    def _find_operation(self, text: str, op: str) -> bool:
        for syn in self.OPERATION_SYNONYMS.get(op, []):
            if syn in text:
                return True
        return False

    def _extract_fields(self, field_str: str) -> list:

        field_str = re.sub(r'\s+y\s+', ',', field_str)
        # Quita frases comunes de operaciones
        field_str = re.sub(r'(suma el total|proyectar reg|totalParteEntera y totalParteDecimal|ordenar por [^,]+|proyectar campo reg concatenando los valores seg[úu]n la plantilla|sumar el monto de las transacciones|agrupar por fecha)', '', field_str, flags=re.IGNORECASE)
        fields = [f.strip() for f in field_str.split(',') if f.strip()]
        expanded = []
        # Obtener campos válidos del esquema si está disponible
        valid_fields = set()
        if self.dataset_manager and hasattr(self.dataset_manager, 'schemas'):
            # Buscar en todas las colecciones
            for schema in self.dataset_manager.schemas.values():
                if hasattr(schema, 'fields'):
                    valid_fields.update([fname.lower() for fname in schema.fields.keys()])
        for f in fields:
            # Solo agregar si es campo válido o si no hay esquema
            if not valid_fields or f.lower() in valid_fields:
                expanded.extend(self._expand_special_phrases(f))
        return expanded

    def parse_natural_language(self, natural_text: str, collection: str = None, campos_esperados: set = None) -> list:
               
        import datetime
        lines = [l.strip() for l in natural_text.split('\n') if l.strip()]
        pipeline = []

        # REGLA MEJORADA: Proyección avanzada dinámica para dateMascara y reg
        for line in lines:
            # Detecta instrucciones para proyección avanzada
            match_date = re.search(r'Proyecta el campo ([\w]+) como la fecha en formato ([A-Z0-9]+) usando los primeros (\d+) caracteres del campo ([\w]+)', line)
            match_reg = re.search(r'Proyecta el campo ([\w]+) concatenando: ([^,]+), ([^,]+), la fecha en formato ([A-Z0-9]+) usando los primeros (\d+) caracteres del campo ([\w]+), ([^,]+), ([^,]+), un espacio y un salto de línea', line)
            if match_date and match_reg:
                # Extrae parámetros dinámicamente
                campo_dateMascara = match_date.group(1)
                formato_dateMascara = match_date.group(2)
                num_caracteres_dateMascara = int(match_date.group(3))
                campo_origen_dateMascara = match_date.group(4)

                campo_reg = match_reg.group(1)
                concat_1 = match_reg.group(2)
                concat_2 = match_reg.group(3)
                formato_reg = match_reg.group(4)
                num_caracteres_reg = int(match_reg.group(5))
                campo_origen_reg = match_reg.group(6)
                concat_3 = match_reg.group(7)
                concat_4 = match_reg.group(8)

                project_stage = {
                    "$project": {
                        "_id": 0,
                        campo_dateMascara: {
                            "$dateToString": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": {"$substr": [f"${campo_origen_dateMascara}", 0, num_caracteres_dateMascara]}
                                    }
                                },
                                "format": f"%{formato_dateMascara}"
                            }
                        },
                        campo_reg: {
                            "$concat": [
                                concat_1,
                                concat_2,
                                {
                                    "$dateToString": {
                                        "date": {
                                            "$dateFromString": {
                                                "dateString": {"$substr": [f"${campo_origen_reg}", 0, num_caracteres_reg]}
                                            }
                                        },
                                        "format": f"%{formato_reg}"
                                    }
                                },
                                concat_3,
                                concat_4,
                                " ",
                                "\n"
                            ]
                        }
                    }
                }
                pipeline.append(project_stage)
                return pipeline
            # REGLA FLEXIBLE MEJORADA: Pipeline avanzado Devices/ServicePoints/ShipOutCycles/Transactions con sumas condicionales y proyección dinámica
            if re.search(r"desanida|unwind|expandir.*devices.*servicepoints.*shipoutcycles.*transactions|pipeline avanzado|sumas condicionales por moneda|proyecta partes enteras y decimales|campo reg con padding", natural_text, re.IGNORECASE):
                pipeline = [
                    {"$unwind": "$Devices"},
                    {"$unwind": "$Devices.ServicePoints"},
                    {"$unwind": {"path": "$Devices.ServicePoints.ShipOutCycles", "preserveNullAndEmptyArrays": True}},
                    {"$unwind": {"path": "$Devices.ServicePoints.ShipOutCycles.Transactions", "preserveNullAndEmptyArrays": True}},
                    {"$group": {
                        "_id": {
                            "deviceId": "$Devices.Id",
                            "branchCode": "$Devices.BranchCode",
                            "subChannelCode": "$Devices.ServicePoints.ShipOutCycles.SubChannelCode",
                            "shipOutCode": "$Devices.ServicePoints.ShipOutCycles.Code",
                            "currencyCode": "$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode",
                        },
                        "totalSoles": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "PEN"]},
                                    "$Devices.ServicePoints.ShipOutCycles.Transactions.Total", 0
                                ]
                            }
                        },
                        "totalDolares": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "USD"]},
                                    "$Devices.ServicePoints.ShipOutCycles.Transactions.Total", 0
                                ]
                            }
                        }
                    }},
                    {"$group": {
                        "_id": 0,
                        "totalSoles": {"$sum": "$totalSoles"},
                        "totalDolares": {"$sum": "$totalDolares"},
                        "totalRegSoles": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$_id.currencyCode", "PEN"]}, 1, 0
                                ]
                            }
                        },
                        "totalRegDolares": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$_id.currencyCode", "USD"]}, 1, 0
                                ]
                            }
                        }
                    }},
                    {"$project": {
                        "totalParteEnteraSoles": {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 0]},
                        "totalParteDecimalSoles": {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 1]}, "0"]}, "00"]},
                        "totalParteEnteraDolares": {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 0]},
                        "totalParteDecimalDolares": {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 1]}, "0"]}, "00"]},
                        "totalRegSoles": "$totalRegSoles",
                        "totalRegDolares": "$totalRegDolares"
                    }},
                    {"$project": {
                        "_id": 0,
                        "reg": {
                            "$concat": [
                                "9",
                                {"$substrCP": [{"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]}, {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]}}, -15]}, 15]},
                                {"$substrCP": [{"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}, {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}] }}, -15]}, 15]},
                                {"$substrCP": [{"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}, {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}] }}, -15]}, 15]},
                                {"$substr": [{"$concat": ["0000000000000", "$totalParteEnteraSoles", {"$substr": ["$totalParteDecimalSoles", 0, 2]}]}, {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}, -15]}, {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}]},
                                {"$substr": [{"$concat": ["0000000000000", "$totalParteEnteraDolares", {"$substr": ["$totalParteDecimalDolares", 0, 2]}]}, {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}, -15]}, {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}]},
                                "\n"
                            ]
                        }
                    }}
                ]
                return pipeline
            # Compatibilidad con la regla estática anterior (legacy)
            if ("dateMascara" in line and "reg" in line) or (
                "Proyecta el campo dateMascara" in line and "Proyecta el campo reg" in line):
                project_stage = {
                    "$project": {
                        "_id": 0,
                        "dateMascara": {
                            "$dateToString": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": {"$substr": ["$Date", 0, 19]}
                                    }
                                },
                                "format": "%Y%m%d"
                            }
                        },
                        "reg": {
                            "$concat": [
                                "1",
                                "002",
                                {
                                    "$dateToString": {
                                        "date": {
                                            "$dateFromString": {
                                                "dateString": {"$substr": ["$Date", 0, 19]}
                                            }
                                        },
                                        "format": "%Y%m%d%H%M%S"
                                    }
                                },
                                "00",
                                "01",
                                " ",
                                "\n"
                            ]
                        }
                    }
                }
                pipeline.append(project_stage)
                return pipeline
        # --- MEJORA: Detección de expresiones temporales como 'último mes' ---
        lower_text = natural_text.lower()
        temporal_match = None
        if 'último mes' in lower_text or 'ultimo mes' in lower_text:
            temporal_match = 'last_month'
        # Puedes agregar más patrones temporales aquí

        # Buscar campo de fecha principal en la colección (por sinónimos)
        fecha_field = None
        if collection and self.dataset_manager and collection in self.dataset_manager.schemas:
            schema = self.dataset_manager.schemas[collection]
            for fname, fdef in schema.fields.items():
                # Buscar por sinónimos comunes de fecha
                all_syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                if any(s in ['fecha', 'fecha_venta', 'fecha de venta', 'fecha venta', 'fecha_transaccion', 'fechaoperacion', 'fechaventa', 'fechatransaccion', 'fechadeventa', 'fechadeoperacion', 'fecha_compra'] for s in all_syns):
                    fecha_field = fname
                    break
        # Fallback: buscar en FIELD_SYNONYMS si no se encontró
        if not fecha_field and hasattr(self, 'FIELD_SYNONYMS'):
            for canonical, syns in self.FIELD_SYNONYMS.items():
                if canonical.lower().startswith('fecha') or 'fecha' in canonical.lower():
                    fecha_field = canonical
                    break
        # PATCH: Si la colección es 'ventas' y no se encontró campo de fecha, usar 'fecha_venta' explícitamente
        if (not fecha_field) and collection and collection.lower() == 'ventas':
            # Verifica que 'fecha_venta' esté en el esquema
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                if 'fecha_venta' in schema.fields:
                    fecha_field = 'fecha_venta'

        # Si se detectó expresión temporal y hay campo de fecha, y la instrucción pide ventas del último mes, sumar total_venta
        if temporal_match == 'last_month' and fecha_field:
            lower_text = natural_text.lower()
            # Solo activar suma si la colección es ventas y la instrucción pide ventas
            if collection and collection.lower() == 'ventas' and ('ventas' in lower_text or 'venta' in lower_text):
                today = datetime.date.today()
                first_day_this_month = today.replace(day=1)
                last_day_last_month = first_day_this_month - datetime.timedelta(days=1)
                first_day_last_month = last_day_last_month.replace(day=1)
                match_stage = {"$match": {
                    fecha_field: {
                        "$gte": str(first_day_last_month),
                        "$lte": str(last_day_last_month)
                    }
                }}
                # Buscar campo de suma (total_venta)
                total_field = None
                if self.dataset_manager and collection in self.dataset_manager.schemas:
                    schema = self.dataset_manager.schemas[collection]
                    for fname, fdef in schema.fields.items():
                        all_syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                        if 'total_venta' in all_syns or 'total' in all_syns or 'importe' in all_syns or 'monto' in all_syns:
                            total_field = fname
                            break
                if not total_field:
                    total_field = 'total_venta'  # Fallback
                # Si los datos pueden estar como string, agrega conversión en el pipeline
                add_fields_stage = {
                    "$addFields": {
                        "fecha_venta_date": {"$dateFromString": {"dateString": f"${fecha_field}"}},
                        "total_venta_num": {"$toDouble": f"${total_field}"}
                    }
                }
                match_stage = {"$match": {
                    "fecha_venta_date": {
                        "$gte": {"$dateFromString": {"dateString": str(first_day_last_month)}},
                        "$lte": {"$dateFromString": {"dateString": str(last_day_last_month)}}
                    }
                }}
                group_stage = {"$group": {"_id": None, "total_venta": {"$sum": "$total_venta_num"}}}
                project_stage = {"$project": {"total_venta": 1}}
                pipeline.extend([add_fields_stage, match_stage, group_stage, project_stage])
                return pipeline
            else:
                # Comportamiento anterior para otros casos con fecha relativa
                today = datetime.date.today()
                first_day_this_month = today.replace(day=1)
                last_day_last_month = first_day_this_month - datetime.timedelta(days=1)
                first_day_last_month = last_day_last_month.replace(day=1)
                match_stage = {"$match": {
                    fecha_field: {
                        "$gte": str(first_day_last_month),
                        "$lte": str(last_day_last_month)
                    }
                }}
                pipeline.append(match_stage)
                if collection and self.dataset_manager and collection in self.dataset_manager.schemas:
                    schema = self.dataset_manager.schemas[collection]
                    project_fields = {fname: 1 for fname in schema.fields.keys()}
                    if len(pipeline) == 1:
                        pipeline.append({"$project": project_fields})

        # --- REGLAS ESPECÍFICAS PARA PATRONES DE NEGOCIO FRECUENTES ---
        # 1. Conteo por grupo: "cuenta cuántos <entidad> hay en cada <campo>"
        match_count_group = re.search(r'cuenta cu[aá]ntos? ([\wáéíóúüñÁÉÍÓÚÜÑ ]+) hay en cada ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_count_group:
            entidad = match_count_group.group(1).strip()
            campo = match_count_group.group(2).strip()
            # Normalizar campo usando sinónimos
            campo_norm = None
            for canonical, synonyms in self.FIELD_SYNONYMS.items():
                if campo.lower() == canonical.lower() or campo.lower() in [s.lower() for s in synonyms]:
                    campo_norm = canonical
                    break
            if not campo_norm:
                campo_norm = campo
            group_stage = {"$group": {"_id": f"${campo_norm}", "count": {"$sum": 1}}}
            project_stage = {"$project": {campo_norm: "$_id", "count": 1, "_id": 0}}
            pipeline.extend([group_stage, project_stage])
            return pipeline
         # Regla específica para transacciones mayores a un monto en un mes
        match_transacciones_monto_mes = re.search(r'transacciones? mayores? a \$?(\d+)[^\d]*(en|de)?\s*(noviembre|diciembre|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre)?', natural_text, re.IGNORECASE)
        if match_transacciones_monto_mes and collection and collection.lower() == "transactions_collection":
            monto = float(match_transacciones_monto_mes.group(1))
            # mes = match_transacciones_monto_mes.group(4)
            meses = [
                     "enero", "febrero", "marzo", "abril", "mayo", "junio",
                     "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
            texto_lower = natural_text.lower()

            pattern = r'\b(' + '|'.join(re.escape(m) for m in meses) + r')\b'
            m = re.search(pattern, texto_lower, flags=re.IGNORECASE)
            mes = m.group(1) if m else None
            
            print("mes:", mes)
            # Buscar campo de fecha y monto en el esquema
            fecha_field = None
            monto_field = None
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                for fname, fdef in schema.fields.items():
                    syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                    if any(s in ['date', 'fecha', 'fechahora', 'timestamp'] for s in syns):
                        fecha_field = fname
                    if any(s in ['total', 'monto', 'amount'] for s in syns):
                        monto_field = fname
            if not fecha_field:
                fecha_field = 'Date'
            if not monto_field:
                monto_field = 'Total'
            # $unwind para desanidar arrays
            pipeline.append({"$unwind": "$Devices"})
            pipeline.append({"$unwind": "$Devices.ServicePoints"})
            pipeline.append({"$unwind": "$Devices.ServicePoints.ShipOutCycles"})
            pipeline.append({"$unwind": "$Devices.ServicePoints.ShipOutCycles.Transactions"})
            # $match por monto y mes (siempre incluir filtro de mes si se menciona)
            meses_map = {'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12}
            match_dict = {f"Devices.ServicePoints.ShipOutCycles.Transactions.{monto_field}": {"$gt": monto}}
            if mes:
                mes_num = meses_map.get(mes.lower(), None)
                if mes_num:
                    match_dict["$expr"] = {
                        "$eq": [
                            {"$month": f"$Devices.ServicePoints.ShipOutCycles.Transactions.{fecha_field}"},
                            mes_num
                        ]
                    }
            pipeline.append({"$match": match_dict})
            # $project solo con campos válidos y anidados
            project_stage = {"$project": {
                "Date": "$Devices.ServicePoints.ShipOutCycles.Transactions.Date",
                "Total": "$Devices.ServicePoints.ShipOutCycles.Transactions.Total",
                "_id": 0
            }}
            pipeline.append(project_stage)
            return pipeline
         # Nueva regla: "¿Qué clientes compraron más de 5 veces este año?"
        match_clientes_mas_5_ano = re.search(r'(clientes|compradores).*compraron.*(más de|mas de)\s*(\d+)\s*veces.*(este año|en \d{4})', natural_text, re.IGNORECASE)
        if match_clientes_mas_5_ano and collection and collection.lower() == "ventas":
            # Detectar campo de fecha y cliente usando nombres canónicos del esquema
            fecha_field = 'fecha_venta'
            cliente_field = 'cliente_id'
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                # Buscar por sinónimos, pero priorizar los nombres canónicos
                for fname, fdef in schema.fields.items():
                    all_syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                    if any(s in ['fecha', 'fecha_venta', 'fecha de venta', 'fecha venta', 'fecha_transaccion', 'fechaoperacion', 'fechaventa', 'fechatransaccion', 'fechadeventa', 'fechadeoperacion', 'fecha_compra'] for s in all_syns):
                        fecha_field = fname
                    if any(s in ['cliente_id', 'cliente'] for s in all_syns):
                        cliente_field = fname
            # Detectar año
            year_match = re.search(r'en (\d{4})', natural_text)
            if year_match:
                year = int(year_match.group(1))
            else:
                year = datetime.date.today().year
            # Detectar cantidad
            cantidad_match = re.search(r'(más de|mas de)\s*(\d+)\s*veces', natural_text)
            if cantidad_match:
                min_veces = int(cantidad_match.group(2))
            else:
                min_veces = 5
            # $match por año
            match_stage = {"$match": {
                fecha_field: {"$regex": f"^{year}-"}
            }}
            # $group por cliente
            group_stage = {"$group": {
                "_id": f"${cliente_field}",
                "num_compras": {"$sum": 1}
            }}
            # $match para filtrar clientes con más de min_veces compras
            match_count_stage = {"$match": {"num_compras": {"$gt": min_veces}}}
            # $project para mostrar cliente y num_compras usando nombres canónicos
            project_stage = {"$project": {cliente_field: "$_id", "num_compras": 1, "_id": 0}}
            pipeline.extend([match_stage, group_stage, match_count_stage, project_stage])
            return pipeline

            # Nueva regla: "¿Cuántas ventas se hicieron por canal online en el último trimestre?"
        match_canal_trimestre = re.search(r'(cu[aá]ntas ventas|cu[aá]ntos|total de ventas).*canal\s+online.*(trimestre|último trimestre|quarter)', natural_text, re.IGNORECASE)
        if match_canal_trimestre and collection and collection.lower() == "ventas":
                # Detectar campo de fecha
                fecha_field = None
                canal_field = None
                if self.dataset_manager and collection in self.dataset_manager.schemas:
                    schema = self.dataset_manager.schemas[collection]
                    for fname, fdef in schema.fields.items():
                        all_syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                        if any(s in ['fecha', 'fecha_venta', 'fecha de venta', 'fecha venta', 'fecha_transaccion', 'fechaoperacion', 'fechaventa', 'fechatransaccion', 'fechadeventa', 'fechadeoperacion', 'fecha_compra'] for s in all_syns):
                            fecha_field = fname
                        if any(s in ['canal', 'canal_venta', 'canal_transaccion', 'canalcompra', 'canalventa'] for s in all_syns):
                            canal_field = fname
                if not fecha_field:
                    fecha_field = 'fecha_venta'
                if not canal_field:
                    canal_field = 'canal'
                # Calcular fechas del último trimestre
                today = datetime.date.today()
                current_month = today.month
                current_year = today.year
                # Trimestre actual
                if current_month in [1,2,3]:
                    start_month = 1
                elif current_month in [4,5,6]:
                    start_month = 4
                elif current_month in [7,8,9]:
                    start_month = 7
                else:
                    start_month = 10
                # Último trimestre
                if start_month == 1:
                    last_q_start = datetime.date(current_year-1, 10, 1)
                    last_q_end = datetime.date(current_year-1, 12, 31)
                else:
                    last_q_start = datetime.date(current_year, start_month-3, 1)
                    last_q_end = datetime.date(current_year, start_month-1, 1) + datetime.timedelta(days=31)
                    last_q_end = last_q_end.replace(day=1) - datetime.timedelta(days=1)
                # $match por canal online y fechas del último trimestre
                match_stage = {"$match": {
                    canal_field: {"$regex": "online", "$options": "i"},
                    fecha_field: {"$gte": str(last_q_start), "$lte": str(last_q_end)}
                }}
                group_stage = {"$group": {"_id": f"${canal_field}", "total_ventas": {"$sum": 1}}}
                # El campo 'canal' no existe en el esquema, pero sí en $_id del $group
                project_stage = {"$project": {f"{canal_field}": "$_id", "total_ventas": 1, "_id": 0}}
                pipeline.extend([match_stage, group_stage, project_stage])
                return pipeline
            # Regla específica: total de ventas por producto y ciudad en [año]
        match_total_ventas_campos_anio = re.search(r"total de ventas por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)(?: y ([\wáéíóúüñÁÉÍÓÚÜÑ_]+))? en (\d{4})", natural_text.lower())
        if match_total_ventas_campos_anio and collection and collection.lower() == "ventas":
            anio = int(match_total_ventas_campos_anio.group(3))
            campos = [match_total_ventas_campos_anio.group(1)]
            if match_total_ventas_campos_anio.group(2):
                campos.append(match_total_ventas_campos_anio.group(2))
            # Detectar campo de fecha
            fecha_field = None
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                for fname, fdef in schema.fields.items():
                    all_syns = [fname.lower()] + [s.lower() for s in getattr(fdef, 'synonyms', [])]
                    if any(s in ['fecha', 'fecha_venta', 'fecha de venta', 'fecha venta', 'fecha_transaccion', 'fechaoperacion', 'fechaventa', 'fechatransaccion', 'fechadeventa', 'fechadeoperacion', 'fecha_compra'] for s in all_syns):
                        fecha_field = fname
                        break
            if not fecha_field:
                fecha_field = 'fecha_venta'
            # $match por año
            match_stage = {"$match": {
                fecha_field: {
                    "$regex": f"^{anio}-"
                }
            }}
            # $group por campos dinámicos
            campos_group = {}
            for campo in campos:
                campo_norm = self._normalize_field(campo, collection=collection)
                campos_group[campo_norm] = f"${campo_norm}"
            total_field = self._normalize_field('total_venta', collection=collection)
            group_stage = {"$group": {
                "_id": campos_group,
                "total_ventas": {"$sum": f"${total_field}"}
            }}
            # $project dinámico
            project_fields = {campo: f"$_id.{self._normalize_field(campo, collection=collection)}" for campo in campos}
            project_fields["total_ventas"] = 1
            project_fields["_id"] = 0
            project_stage = {"$project": project_fields}
            pipeline.extend([match_stage, group_stage, project_stage])
            return pipeline

        # 2. Suma total por grupo: "calcula el total de <campo_suma> por <campo_grupo>"
        match_sum_group = re.search(r'calcula el total de ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_sum_group:
            campo_suma = match_sum_group.group(1).strip()
            campo_grupo = match_sum_group.group(2).strip()
            # Normalizar campos
            campo_suma_norm = self._normalize_field(campo_suma, collection=collection)
            campo_grupo_norm = self._normalize_field(campo_grupo, collection=collection)
            group_stage = {"$group": {"_id": f"${campo_grupo_norm}", f"total_{campo_suma_norm}": {"$sum": f"${campo_suma_norm}"}}}
            project_stage = {"$project": {campo_grupo_norm: "$_id", f"total_{campo_suma_norm}": 1, "_id": 0}}
            pipeline.extend([group_stage, project_stage])
            return pipeline

        # 3. Promedio por grupo: "muestra el precio promedio de los <entidad>" o "promedio de <campo> por <grupo>"
        match_avg_group = re.search(r'(?:promedio|precio promedio) de ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_avg_group:
            campo_avg = match_avg_group.group(1).strip()
            campo_grupo = match_avg_group.group(2).strip()
            campo_avg_norm = self._normalize_field(campo_avg, collection=collection)
            campo_grupo_norm = self._normalize_field(campo_grupo, collection=collection)
            group_stage = {"$group": {"_id": f"${campo_grupo_norm}", f"avg_{campo_avg_norm}": {"$avg": f"${campo_avg_norm}"}}}
            project_stage = {"$project": {campo_grupo_norm: "$_id", f"avg_{campo_avg_norm}": 1, "_id": 0}}
            pipeline.extend([group_stage, project_stage])
            return pipeline

        # 3b. Multi-field group with total and average: soporta variantes de redacción
        match_multi_group = re.search(
            r'(agru(p|pe)[\w\s]*ventas[\w\s]*por ([\wáéíóúüñÁÉÍÓÚÜÑ_,\sy]+)[,\s]*(mostrando|y)?[\w\s]*(total|suma|sumatoria)?[\w\s]*(y|e)?[\w\s]*(promedio|media)?[\w\s]*(por grupo)?)',
            natural_text.lower()
        )
        if match_multi_group and collection and collection.lower() == "ventas":
            # Extraer campos de agrupación
            campos_match = re.search(r'por ([\wáéíóúüñÁÉÍÓÚÜÑ_,\sy]+?)(?:,| mostrando| y |$)', natural_text.lower())
            if campos_match:
                campos = campos_match.group(1)
                campos = [c.strip() for c in re.split(r',| y ', campos) if c.strip()]
            else:
                campos = []
            # Normalizar campos
            campos_norm = [self._normalize_field(c, collection=collection) for c in campos]
            # Detectar campo de total (usualmente total_venta o similar)
            total_field = self._normalize_field('total_venta', collection=collection)
            # Detectar campo de fecha para extraer mes si corresponde
            fecha_field = self._normalize_field('fecha_venta', collection=collection)
            # Si uno de los campos es mes, crear campo mes a partir de fecha
            add_fields_stage = None
            group_id = {}
            project_fields = {}
            for c, c_norm in zip(campos, campos_norm):
                if c_norm in ['mes', 'month']:
                    add_fields_stage = {"$addFields": {"mes": {"$month": f"${fecha_field}"}}}
                    group_id['mes'] = "$mes"
                    project_fields['mes'] = "$_id.mes"
                else:
                    group_id[c_norm] = f"${c_norm}"
                    project_fields[c_norm] = f"$_id.{c_norm}"
            group_stage = {"$group": {"_id": group_id, "total": {"$sum": f"${total_field}"}, "promedio": {"$avg": f"${total_field}"}}}
            project_fields['total'] = 1
            project_fields['promedio'] = 1
            project_fields['_id'] = 0
            project_stage = {"$project": project_fields}
            if add_fields_stage:
                pipeline.extend([add_fields_stage, group_stage, project_stage])
            else:
                pipeline.extend([group_stage, project_stage])
            return pipeline


        # 4. Agrupación y orden por monto descendente: "Dame las ventas por producto, agrupadas por cliente y ordenadas por monto descendente"
        match_group_sort = re.search(r'ventas por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+),? agrupadas? por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) y ordenadas? por (monto|total|importe|valor) descendente', natural_text, re.IGNORECASE)
        if match_group_sort and collection and collection.lower() == "ventas":
            campo1 = match_group_sort.group(1).strip()
            campo2 = match_group_sort.group(2).strip()
            campo_monto = match_group_sort.group(3).strip()
            # Normalizar campos
            campo1_norm = self._normalize_field(campo1, collection=collection)
            campo2_norm = self._normalize_field(campo2, collection=collection)
            monto_norm = self._normalize_field('total_venta', collection=collection)
            # $group por cliente y producto
            group_stage = {"$group": {"_id": {campo2_norm: f"${campo2_norm}", campo1_norm: f"${campo1_norm}"}, "total_venta": {"$sum": f"${monto_norm}"}}}
            # $sort por total_venta descendente
            sort_stage = {"$sort": {"total_venta": -1}}
            # $project para mostrar campos relevantes
            project_stage = {"$project": {campo2_norm: "$_id." + campo2_norm, campo1_norm: "$_id." + campo1_norm, "total_venta": 1, "_id": 0}}
            pipeline.extend([group_stage, sort_stage, project_stage])
            return pipeline

        # 4. Top-N: "muestra los N <entidad> más vendidos"
        match_top_n = re.search(r'muestra los (\d+) ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) m[aá]s vendidos', natural_text, re.IGNORECASE)
        if match_top_n:
            n = int(match_top_n.group(1))
            entidad = match_top_n.group(2).strip()
            # Buscar campos típicos
            field_producto = self._normalize_field(entidad, collection=collection)
            field_cantidad = self._normalize_field('ventas', collection=collection)
            group_stage = {"$group": {"_id": f"${field_producto}", "total_ventas": {"$sum": f"${field_cantidad}"}}}
            sort_stage = {"$sort": {"total_ventas": -1}}
            limit_stage = {"$limit": n}
            project_stage = {"$project": {entidad: "$_id", "total_ventas": 1, "_id": 0}}
            pipeline.extend([group_stage, sort_stage, limit_stage, project_stage])
            return pipeline

        # 5. Máximo por grupo: "cuál es el <entidad> con más <campo>"
        match_max_group = re.search(r'cu[aá]l es el ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) con m[aá]s ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_max_group:
            entidad = match_max_group.group(1).strip()
            campo = match_max_group.group(2).strip()
            field_entidad = self._normalize_field(entidad, collection=collection)
            field_campo = self._normalize_field(campo, collection=collection)
            group_stage = {"$group": {"_id": f"${field_entidad}", f"total_{field_campo}": {"$sum": f"${field_campo}"}}}
            sort_stage = {"$sort": {f"total_{field_campo}": -1}}
            limit_stage = {"$limit": 1}
            project_stage = {"$project": {entidad: "$_id", f"total_{field_campo}": 1, "_id": 0}}
            pipeline.extend([group_stage, sort_stage, limit_stage, project_stage])
            return pipeline

        # 6. Conteo por campo: "cuenta cuántos <entidad> hay por <campo>"
        match_count_by = re.search(r'cuenta cu[aá]ntos? ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) hay por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_count_by:
            entidad = match_count_by.group(1).strip()
            campo = match_count_by.group(2).strip()
            field_campo = self._normalize_field(campo, collection=collection)
            group_stage = {"$group": {"_id": f"${field_campo}", "count": {"$sum": 1}}}
            project_stage = {"$project": {campo: "$_id", "count": 1, "_id": 0}}
            pipeline.extend([group_stage, project_stage])
            return pipeline

        # 7. Lista por grupo: "lista los <entidad> por <campo>"
        match_list_by = re.search(r'lista los ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) por ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', natural_text, re.IGNORECASE)
        if match_list_by:
            entidad = match_list_by.group(1).strip()
            campo = match_list_by.group(2).strip()
            field_entidad = self._normalize_field(entidad, collection=collection)
            field_campo = self._normalize_field(campo, collection=collection)
            group_stage = {"$group": {"_id": f"${field_campo}", f"{field_entidad}s": {"$push": f"${field_entidad}"}}}
            project_stage = {"$project": {campo: "$_id", f"{field_entidad}s": 1, "_id": 0}}
            pipeline.extend([group_stage, project_stage])
            return pipeline
        # --- FIN REGLAS ESPECÍFICAS ---
        # --- MEJORA: Usar campos del esquema real si está disponible ---
        if self.dataset_manager and collection in self.dataset_manager.schemas:
            schema_fields = set(self.dataset_manager.schemas[collection].fields.keys())
        else:
            schema_fields = set(['nombres', 'apellidos', 'productos', 'categoría', 'precio'])
        # Normalización robusta para comparar campos
        normaliza_campo_robusto = self.normaliza_campo_robusto
        campos_mencionados = set()
        for field in schema_fields:
            # Buscar el campo literal o variantes en la consulta (normalizado)
            field_norm = normaliza_campo_robusto(field)
            pattern = rf'\b{field}\b'
            matches = re.findall(pattern, natural_text, re.IGNORECASE)
            if matches:
                if len(matches) / max(1, len(natural_text.split())) >= self.threshold:
                    campos_mencionados.add(field)
        # Incluir también los campos esperados si se pasan explícitamente (normalizado)
        if campos_esperados:
            campos_esperados_norm = set()
            for c in campos_esperados:
                for f in schema_fields:
                    if normaliza_campo_robusto(c) == normaliza_campo_robusto(f):
                        campos_esperados_norm.add(f)
            campos_mencionados.update(campos_esperados_norm)

        # Si hay campos mencionados y ya existe un $project, añadirlos si faltan (solo si son válidos)
        for stage in pipeline:
            if "$project" in stage:
                for field in campos_mencionados:
                    if field not in stage["$project"] and field in schema_fields:
                        stage["$project"][field] = 1
                # Eliminar del $project cualquier campo que no sea válido
                campos_invalidos = [k for k in list(stage["$project"].keys()) if k not in schema_fields]
                for k in campos_invalidos:
                    del stage["$project"][k]
                # Si se eliminaron todos los campos y no queda ninguno válido, sugerir los campos válidos
                if len(stage["$project"]) == 0 and len(campos_invalidos) > 0:
                    # Sugerir campos válidos en la colección
                    for f in schema_fields:
                        stage["$project"][f] = 1
                    stage["$project"]["campo_no_encontrado"] = 1
                break

        # Si hay campos mencionados y no hay $project, crear uno solo con válidos
        if campos_mencionados and not any("$project" in stage for stage in pipeline):
            campos_filtrados = [field for field in campos_mencionados if field in schema_fields]
            if campos_filtrados:
                project_stage = {"$project": {field: 1 for field in campos_filtrados}}
                pipeline.append(project_stage)
                

        # Filtrar cualquier $project generado en el pipeline para que solo tenga campos válidos
        for stage in pipeline:
            if "$project" in stage:
                keys_a_borrar = [k for k in stage["$project"].keys() if k not in schema_fields]
                for k in keys_a_borrar:
                    del stage["$project"][k]

        # Si se genera un $project en cualquier otro punto, filtrar sus campos también
        for i, stage in enumerate(pipeline):
            if "$project" in stage:
                filtered = {k: v for k, v in stage["$project"].items() if k in schema_fields}
                stage["$project"] = filtered

        # Refuerzo: limpiar cualquier $project existente para que solo tenga campos válidos
        for stage in pipeline:
            if "$project" in stage:
                keys_a_borrar = [k for k in stage["$project"].keys() if k not in schema_fields]
                for k in keys_a_borrar:
                    del stage["$project"][k]
        # Normalizar nombre de colección si es necesario
        if collection:
            collection = self._normalize_collection(collection)
        # --- Proyectar los primeros N caracteres de un campo ---
        for line in lines:
            # Caso especial: crear campo montoRedondeado que sea el total redondeado a dos decimales
            match_redondeo = re.search(r'crear campo (montoRedondeado|monto_redondeado) que sea el total redondeado a dos decimales', line, re.IGNORECASE)
            if match_redondeo:
                project_stage = {
                    "$project": {
                        "montoRedondeado": {"$round": ["$Total", 2]},
                        "_id": 0
                    }
                }
                print(f"[DEBUG] Pipeline generado: {project_stage}")
                return [project_stage]
            # Caso especial: filtra registros donde el campo Total sea nulo o menor a 0
            match_nulo_menor = re.search(r'filtra registros donde el campo ([\w\.]+) sea nulo o menor a 0', line, re.IGNORECASE)
            if match_nulo_menor:
                raw_field = match_nulo_menor.group(1).strip()
                field = self._normalize_field(raw_field, collection=collection)
                # Fuerza la ruta anidada si es 'total' o 'Total' y la colección es transacciones
                if (raw_field.lower() == 'total' or field.lower() == 'total') and collection and collection.lower().startswith('transac'):
                    field = 'Devices.ServicePoints.ShipOutCycles.Transactions.Total'
                field_path = field
                match_stage = {
                    "$match": {
                        "$or": [
                            {field_path: {"$eq": None}},
                            {field_path: {"$lt": 0}}
                        ]
                    }
                }
                print(f"[DEBUG] Pipeline generado: {match_stage}")
                return [match_stage]
            match_substr = re.search(r"proyecta los primeros (\d+) caracteres del campo ([\w_]+)", line, re.IGNORECASE)
            if match_substr:
                n = int(match_substr.group(1))
                campo = match_substr.group(2)
                project_stage = {"$project": {campo: {"$substr": [f"${campo}", 0, n]}, "_id": 0}}
                pipeline.append(project_stage)
                return pipeline
        pipeline = []  # Inicializa antes de cualquier uso
        # Heurística: detectar instrucción de substr en campo string
        substr_match = re.search(r"proyectar los caracteres de la posición (\d+) en adelante del ([a-zA-Z0-9_]+)", natural_text)
        if substr_match:
            pos = int(substr_match.group(1)) - 1  # MongoDB es base 0
            campo = substr_match.group(2)
            pipeline = [
                {
                    "$project": {
                        campo: {"$substrBytes": [f"${campo}", pos, {"$strLenBytes": f"${campo}"}]}
                    }
                }
            ]
            return pipeline

        # --- NUEVO: Si la colección es ventas o clientes y no existe, crear con datos mínimos ---
        if self.dataset_manager:
            if collection == 'ventas' and collection not in self.dataset_manager.schemas:
                self.dataset_manager.create_schema('ventas', 'Colección de ventas de ejemplo')
                # Agrega campos necesarios para el join
                from dataset_manager import FieldDefinition
                self.dataset_manager.add_field('ventas', FieldDefinition(name='cliente_id', type='number', path='cliente_id', description='ID del cliente', examples=['101'], synonyms=['cliente_id']))
                self.dataset_manager.add_field('ventas', FieldDefinition(name='total_venta', type='number', path='total_venta', description='Total de la venta', examples=['500'], synonyms=['total_venta']))
                self.dataset_manager.add_sample_document('ventas', {'_id': 1, 'cliente_id': 101, 'total_venta': 500})
                self.dataset_manager.add_sample_document('ventas', {'_id': 2, 'cliente_id': 102, 'total_venta': 700})
            if collection == 'clientes' and collection not in self.dataset_manager.schemas:
                self.dataset_manager.create_schema('clientes', 'Colección de clientes de ejemplo')
                from dataset_manager import FieldDefinition
                self.dataset_manager.add_field('clientes', FieldDefinition(name='cliente_id', type='number', path='cliente_id', description='ID del cliente', examples=['101'], synonyms=['cliente_id']))
                self.dataset_manager.add_field('clientes', FieldDefinition(name='nombre_cliente', type='string', path='nombre_cliente', description='Nombre del cliente', examples=['Juan'], synonyms=['nombre_cliente']))
                self.dataset_manager.add_sample_document('clientes', {'_id': 101, 'cliente_id': 101, 'nombre_cliente': 'Juan'})
                self.dataset_manager.add_sample_document('clientes', {'_id': 102, 'cliente_id': 102, 'nombre_cliente': 'Ana'})

        # --- NUEVO: Si la instrucción requiere join con clientes y no existe, crear ---
        if self.dataset_manager and 'clientes' in natural_text and 'clientes' not in self.dataset_manager.schemas:
            self.dataset_manager.create_schema('clientes', 'Colección de clientes de ejemplo')
            from dataset_manager import FieldDefinition
            self.dataset_manager.add_field('clientes', FieldDefinition(name='cliente_id', type='number', path='cliente_id', description='ID del cliente', examples=['101'], synonyms=['cliente_id']))
            self.dataset_manager.add_field('clientes', FieldDefinition(name='nombre_cliente', type='string', path='nombre_cliente', description='Nombre del cliente', examples=['Juan'], synonyms=['nombre_cliente']))
            self.dataset_manager.add_sample_document('clientes', {'_id': 101, 'cliente_id': 101, 'nombre_cliente': 'Juan'})
            self.dataset_manager.add_sample_document('clientes', {'_id': 102, 'cliente_id': 102, 'nombre_cliente': 'Ana'})

        # --- NUEVO: Soporte dinámico para 'filtra clientes que no hayan realizado compras en el último año', 'penúltimo año', 'hace N años', etc. ---
        import datetime
        for line in [l.strip() for l in natural_text.split('\n') if l.strip()]:
            # último año
            match_ultimo = re.search(r"filtra clientes? que no hayan realizado compras? en el (último|ultimo) año", line, re.IGNORECASE)
            # penúltimo año
            match_penultimo = re.search(r"filtra clientes? que no hayan realizado compras? en el (pen[uú]ltimo) año", line, re.IGNORECASE)
            # hace N años
            match_hace_n = re.search(r"filtra clientes? que no hayan realizado compras? hace (\d+) años", line, re.IGNORECASE)
            if match_ultimo:
                hoy = datetime.datetime.now()
                inicio = (hoy - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
                campo_compras = self._normalize_field('compras', collection=collection)
                campo_fecha = self._normalize_field('fecha', collection=collection)
                if campo_compras == 'compras': campo_compras = 'compras'
                if campo_fecha == 'fecha': campo_fecha = 'fecha'
                match_stage = {
                    "$match": {
                        campo_compras: {
                            "$not": {
                                "$elemMatch": {
                                    campo_fecha: {"$gte": inicio}
                                }
                            }
                        }
                    }
                }
                return [match_stage]
            elif match_penultimo:
                hoy = datetime.datetime.now()
                inicio = (hoy - datetime.timedelta(days=365*2)).strftime("%Y-%m-%d")
                fin = (hoy - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
                campo_compras = self._normalize_field('compras', collection=collection)
                campo_fecha = self._normalize_field('fecha', collection=collection)
                if campo_compras == 'compras': campo_compras = 'compras'
                if campo_fecha == 'fecha': campo_fecha = 'fecha'
                match_stage = {
                    "$match": {
                        campo_compras: {
                            "$not": {
                                "$elemMatch": {
                                    campo_fecha: {"$gte": inicio, "$lt": fin}
                                }
                            }
                        }
                    }
                }
                return [match_stage]
            elif match_hace_n:
                hoy = datetime.datetime.now()
                n = int(match_hace_n.group(1))
                inicio = (hoy - datetime.timedelta(days=365*(n))).strftime("%Y-%m-%d")
                fin = (hoy - datetime.timedelta(days=365*(n-1))).strftime("%Y-%m-%d")
                campo_compras = self._normalize_field('compras', collection=collection)
                campo_fecha = self._normalize_field('fecha', collection=collection)
                if campo_compras == 'compras': campo_compras = 'compras'
                if campo_fecha == 'fecha': campo_fecha = 'fecha'
                match_stage = {
                    "$match": {
                        campo_compras: {
                            "$not": {
                                "$elemMatch": {
                                    campo_fecha: {"$gte": inicio, "$lt": fin}
                                }
                            }
                        }
                    }
                }
                return [match_stage]

        # --- NUEVO: Soporte para 'agrega la suma total de ventas por mes' ---
        suma_mes_match = re.search(r'agrega la suma total de ventas por mes', natural_text, re.IGNORECASE)
        if suma_mes_match:
            field_fecha = self._normalize_field('fecha', collection=collection)
            if field_fecha == 'fecha':
                field_fecha = self._normalize_field('date', collection=collection)
            field_total = self._normalize_field('total', collection=collection)
            add_fields_stage = {
                "$addFields": {
                    "anio_mes": {"$substr": [f"${field_fecha}", 0, 7]}
                }
            }
            group_stage = {
                "$group": {
                    "_id": "$anio_mes",
                    "suma_total_ventas": {"$sum": f"${field_total}"}
                }
            }
            sort_stage = {"$sort": {"_id": 1}}
            project_stage = {"$project": {"mes": "$_id", "suma_total_ventas": 1, "_id": 0}}
            pipeline.extend([add_fields_stage, group_stage, sort_stage, project_stage])
            return pipeline
        # --- NUEVO: Soporte para JOIN explícito ---
        join_match = re.search(r'une la colección (\w+) con (?:la colección )?(\w+) usando (?:el campo )?([\w\.]+)(?: y proyecta ([\w, _]+))?', natural_text, re.IGNORECASE)
        if join_match:
            import logging
            origen = join_match.group(1)
            destino = join_match.group(2)
            campo_union = join_match.group(3).strip()
            campos_proy_raw = join_match.group(4) if join_match.lastindex >= 4 else None
            # Separar correctamente los campos de proyección si existen
            campos_proy = [c.strip() for c in re.split(r',|y', campos_proy_raw) if c.strip()] if campos_proy_raw else []
            # Validar existencia de colecciones y campo de unión
            colecciones_validas = origen in self.dataset_manager.schemas and destino in self.dataset_manager.schemas
            campo_valido_origen = self._validate_field_with_dataset(campo_union, collection_name=origen)
            campo_valido_destino = self._validate_field_with_dataset(campo_union, collection_name=destino)
            if not colecciones_validas:
                logging.warning(f"[WARNING] Colección origen o destino no existe en el dataset: {origen}, {destino}. Se generará el pipeline igualmente.")
                # No se lanza excepción, solo warning
            if not (campo_valido_origen and campo_valido_destino):
                logging.warning(f"[WARNING] Campo de unión '{campo_union}' no existe en ambas colecciones: {origen}, {destino}. Se generará el pipeline igualmente.")
                # No se lanza excepción, solo warning
            # $lookup
            lookup_stage = {
                "$lookup": {
                    "from": destino,
                    "localField": campo_union,
                    "foreignField": campo_union,
                    "as": f"{destino}_info"
                }
            }
            pipeline.append(lookup_stage)
            # $unwind
            pipeline.append({"$unwind": f"${destino}_info"})
            # $project si hay campos
            if campos_proy:
                project_stage = {"$project": {c: 1 for c in campos_proy}}
                pipeline.append(project_stage)
            # Si no hay campos a proyectar, igual retorna el pipeline con lookup y unwind
            logging.info(f"Pipeline generado para JOIN: {pipeline}")
            return pipeline

        # --- MEJORA: Soporte para 'lista los productos más vendidos esta semana' y variantes ---
        top_vendidos_semana_match = re.search(r'(lista|muestra) (los )?(?P<n>\d+)? ?productos m[aá]s vendidos( esta semana)?', natural_text, re.IGNORECASE)
        if top_vendidos_semana_match:
            import datetime
            n = top_vendidos_semana_match.group('n')
            top_n = int(n) if n else 10  # Por defecto top 10 si no se especifica
            # Si la colección es 'productos', pero el campo 'total_venta' no existe, forzar a 'ventas'
            schema_fields = set()
            original_collection = collection
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema_fields = set(self.dataset_manager.schemas[collection].fields.keys())
            if (collection == 'productos' or collection.lower() == 'productos') and 'total_venta' not in schema_fields:
                if 'ventas' in self.dataset_manager.schemas:
                    collection = 'ventas'
                    schema_fields = set(self.dataset_manager.schemas[collection].fields.keys())
            # Determinar rango de la semana actual
            today = datetime.date.today()
            start_of_week = today - datetime.timedelta(days=today.weekday())
            end_of_week = start_of_week + datetime.timedelta(days=6)
            # Buscar campos relevantes
            field_producto = self._normalize_field('producto', collection=collection)
            field_cantidad = self._normalize_field('total_venta', collection=collection)
            # Validar que el campo producto existe en el esquema de la colección
            if field_producto not in schema_fields:
                # Buscar un campo similar (por ejemplo, 'producto' en ventas, 'nombre' en productos)
                for f in schema_fields:
                    if 'producto' in f:
                        field_producto = f
                        break
            # Filtro por semana
            match_stage = {"$match": {
                "fecha_venta": {
                    "$gte": str(start_of_week),
                    "$lte": str(end_of_week)
                }
            }}
            # Agrupar por producto y sumar total_venta
            group_stage = {"$group": {"_id": f"${field_producto}", "total_venta": {"$sum": f"${field_cantidad}"}}}
            sort_stage = {"$sort": {"total_venta": -1}}
            limit_stage = {"$limit": top_n}
            # El nombre del campo proyectado debe coincidir con el campo real
            project_stage = {"$project": {field_producto: "$_id", "total_venta": 1, "_id": 0}}
            pipeline.extend([match_stage, group_stage, sort_stage, limit_stage, project_stage])
            return pipeline

        lines = [l.strip() for l in natural_text.split('\n') if l.strip()]

        # --- Feature engineering: crear campo fechaFormateada que convierta el campo Date a formato ISO ---
        for line in lines:
            match_fecha = re.search(r"crear campo (fechaformateada|fecha_formateada) que convierta el campo (date|fecha) a formato (iso|%Y-%m-%dT%H:%M:%S|%Y-%m-%d)", line, re.IGNORECASE)
            if match_fecha:
                campo_destino = match_fecha.group(1)
                campo_origen = match_fecha.group(2)
                formato = match_fecha.group(3)
                if formato.lower() == "iso":
                    formato = "%Y-%m-%dT%H:%M:%S"
                expr = {
                    "$dateToString": {
                        "date": {
                            "$dateFromString": {
                                "dateString": f"${campo_origen}"
                            }
                        },
                        "format": formato
                    }
                }
                project_stage = {"$project": {campo_destino: expr, "_id": 0}}
                pipeline.append(project_stage)
                return pipeline

        # --- Filtro por caracteres especiales en nombre de producto ---
        for line in lines:
            match_regex = re.search(r"filtra registros cuyo nombre de producto contenga caracteres especiales", line, re.IGNORECASE)
            if match_regex:
                regex = r"[^a-zA-Z0-9\s]"
                match_stage = {"$match": {"nombre_producto": {"$regex": regex}}}
                pipeline.append(match_stage)
                return pipeline

        # --- NUEVO: Soporte para filtro por rango de fechas ---
        for line in lines:
            date_range_match = re.search(r"filtra [\wáéíóúüñÁÉÍÓÚÜÑ ]*entre (\d{4}-\d{2}-\d{2}) y (\d{4}-\d{2}-\d{2})", line, re.IGNORECASE)
            if date_range_match:
                fecha_ini = date_range_match.group(1)
                fecha_fin = date_range_match.group(2)
                # Normalizar campo de fecha (puede ser 'Date' o ruta anidada)
                field_date = self._normalize_field('date', collection=collection)
                match_stage = {"$match": {field_date: {"$gte": fecha_ini, "$lte": fecha_fin}}}
                # Desanidar si es transacciones
                if collection and collection.lower().startswith('transac'):
                    return [
                        match_stage,
                        {"$unwind": "$Devices"},
                        {"$unwind": "$Devices.ServicePoints"},
                        {"$unwind": "$Devices.ServicePoints.ShipOutCycles"},
                        {"$unwind": "$Devices.ServicePoints.ShipOutCycles.Transactions"}
                    ]
                else:
                    return [match_stage]

        # --- NUEVO: Soporte para $match con $regex para instrucciones tipo 'busca empleados cuyo nombre comience con ...' ---
        for line in lines:
            regex_match = re.search(r"busca [\wáéíóúüñÁÉÍÓÚÜÑ ]+ cuyo ([\wáéíóúüñÁÉÍÓÚÜÑ_]+) comience con '([^']+)'", line, re.IGNORECASE)
            if regex_match:
                field = regex_match.group(1).strip()
                value = regex_match.group(2)
                field_norm = self._normalize_field(field, collection=collection)
                match_stage = {"$match": {field_norm: {"$regex": f"^{value}", "$options": "i"}}}
                return [match_stage]

        # --- NUEVO: Soporte para conteo por grupo (ej: cuenta cuántos empleados hay en cada departamento) ---
        for line in lines:
            count_group_match = re.search(r'cuenta cu[aá]ntos? [\wáéíóúüñÁÉÍÓÚÜÑ ]+ hay en cada ([\wáéíóúüñÁÉÍÓÚÜÑ_]+)', line, re.IGNORECASE)
            if count_group_match:
                group_field = count_group_match.group(1).strip()
                # Normalizar el nombre del campo usando sinónimos si es posible
                group_field_norm = None
                for canonical, synonyms in self.FIELD_SYNONYMS.items():
                    if group_field.lower() == canonical.lower() or group_field.lower() in [s.lower() for s in synonyms]:
                        group_field_norm = canonical
                        break
                if not group_field_norm:
                    group_field_norm = group_field
                group_stage = {"$group": {"_id": f"${group_field_norm}", "count": {"$sum": 1}}}
                pipeline.append(group_stage)
                # Opcional: $project para mostrar campo y count
                project_stage = {"$project": {group_field_norm: "$_id", "count": 1, "_id": 0}}
                pipeline.append(project_stage)
                # Ya no es necesario seguir procesando otras reglas para este caso
                break
        # --- FIN NUEVO ---

        # --- NUEVO: Soporte para frases tipo 'del departamento de <nombre>' y proyección de campos ---
        campos_a_proyectar = set()
        for line in lines:
            # Detectar campos a proyectar en frases tipo 'muestra los nombres y apellidos ...'
            proj_match = re.search(r'muestra (los|las)? ([\wáéíóúüñÁÉÍÓÚÜÑ, y]+)', line, re.IGNORECASE)
            if proj_match:
                campos = proj_match.group(2)
                # Separar por 'y', ',' y espacios
                campos = re.split(r',| y | e |\s+', campos)
                campos = [c.strip() for c in campos if c.strip() and c.lower() not in ['de', 'los', 'las', 'empleados', 'empleadas']]
                campos_a_proyectar.update(campos)
            # Filtro por departamento
            dept_match = re.search(r'del departamento de ([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+)', line, re.IGNORECASE)
            if dept_match:
                dept_value = dept_match.group(1).strip()
                # Intenta encontrar el campo de departamento en los sinónimos
                dept_field = None
                for canonical, synonyms in self.FIELD_SYNONYMS.items():
                    if 'departamento' in canonical.lower() or any('departamento' in s.lower() for s in synonyms):
                        rutas = [s for s in synonyms if '.' in s]
                        if rutas:
                            dept_field = max(rutas, key=len)
                        else:
                            dept_field = canonical
                        break
                if not dept_field:
                    dept_field = 'departamento'
                match_stage = {"$match": {dept_field: dept_value}}
                pipeline.append(match_stage)
                # No break: permite otros filtros
        # Si se detectaron campos a proyectar, agregarlos al pipeline
        if campos_a_proyectar:
            # Normalizar nombres de campos usando sinónimos si es posible
            campos_finales = set()
            for campo in campos_a_proyectar:
                campo_norm = None
                for canonical, synonyms in self.FIELD_SYNONYMS.items():
                    if campo.lower() == canonical.lower() or campo.lower() in [s.lower() for s in synonyms]:
                        campo_norm = canonical
                        break
                if not campo_norm:
                    campo_norm = campo
                campos_finales.add(campo_norm)
            project_stage = {"$project": {c: 1 for c in campos_finales}}
            pipeline.append(project_stage)
    # --- FIN NUEVO ---

        # --- NUEVO: Soporte para $sort descendente y ascendente ---
        for line in lines:
            # Ejemplo: ordena los empleados por fecha de ingreso descendente
            sort_match = re.search(r'ordena[\w ]* por ([\w_ ]+) (descendente|ascendente)', line, re.IGNORECASE)
            if sort_match:
                field = sort_match.group(1).strip().replace(' ', '')
                order = sort_match.group(2).lower()
                # Normalizar campo usando sinónimos
                field_norm = self._normalize_field(field, collection=collection)
                sort_dir = -1 if order == 'descendente' else 1
                pipeline.append({'$sort': {field_norm: sort_dir}})
                # No break: permite otros sorts si hay más de uno

        # --- MEJORADO: Soporte para filtros tipo 'filtra registros cuyo <campo> sea mayor/menor/igual/mayor o igual/menor o igual a <valor>' ---
        for line in lines:
            # Usar LLM para decidir operador y valor óptimos
            filter_match = re.search(r'filtra (registros|transacciones)?\s*cu?yo ([\w\. ]+) sea (mayor o igual|menor o igual|mayor|menor|igual) a ([\d\.]+)', line, re.IGNORECASE)
            if filter_match:
                tipo = filter_match.group(1)
                raw_field = filter_match.group(2).strip()
                op = filter_match.group(3).lower()
                value = filter_match.group(4)
                field = self._normalize_field(raw_field, collection=collection)
                if not self._validate_field_with_dataset(field, collection):
                    print(f"[WARN] El campo '{field}' no existe en el esquema de la colección '{collection}'.")
                    continue
                if (raw_field.lower() == 'total' or field.lower() == 'total') and (collection and collection.lower().startswith('transac') or (tipo and 'transaccion' in tipo.lower())):
                    field = 'Devices.ServicePoints.ShipOutCycles.Transactions.Total'
                field_path = field
                # LLM: pedir sugerencia de operador y valor
                llm_prompt = f"Dada la instrucción: '{line}', ¿qué operador MongoDB ($gt, $gte, $lt, $lte, $eq) y valor usarías para el campo '{field_path}'? Responde en formato JSON: {{'operator': '...', 'value': ...}}"
                try:
                    llm_response = self.llm_engine.suggest_operator_and_value(llm_prompt)
                    mongo_op = llm_response.get('operator', '$eq')
                    value_final = llm_response.get('value', value)
                except Exception:
                    # Fallback a lógica tradicional
                    if op == 'mayor':
                        mongo_op = "$gt"
                    elif op == 'mayor o igual':
                        mongo_op = "$gte"
                    elif op == 'menor':
                        mongo_op = "$lt"
                    elif op == 'menor o igual':
                        mongo_op = "$lte"
                    elif op == 'igual':
                        mongo_op = "$eq"
                    else:
                        mongo_op = "$eq"
                    try:
                        value_num = float(value)
                        value_final = round(value_num, 2)
                    except Exception:
                        value_final = value
                match_stage = {"$match": {field_path: {mongo_op: value_final}}}
                print(f"[DEBUG] Pipeline generado: {match_stage}")
                return [match_stage]
        # --- FIN MEJORADO ---
        # PASO 0: Detectar instrucciones de join y agregar $lookup solo si se solicita explícitamente
        join_detected = False
        for line in lines:
            join_match = re.search(r'une la colección ([a-zA-Z0-9_]+) con la colección ([a-zA-Z0-9_]+) usando el campo ([a-zA-Z0-9_]+)', line, re.IGNORECASE)
            if join_match:
                local_collection = join_match.group(1)
                from_collection = join_match.group(2)
                local_field = join_match.group(3)
                # --- NUEVO: Soporte para JOIN explícito ---
                pipeline = []
                join_match = re.search(r'une la colección (\w+) con la colección (\w+) usando el campo ([\w\.]+) proyecta los campos ([\w, _]+)', natural_text, re.IGNORECASE)
                if join_match:
                    origen = join_match.group(1)
                    destino = join_match.group(2)
                    campo_union = join_match.group(3).strip()
                    campos_proy = [c.strip() for c in join_match.group(4).split(',') if c.strip()]
                    # $lookup
                    lookup_stage = {
                        "$lookup": {
                            "from": destino,
                            "localField": campo_union,
                            "foreignField": campo_union,
                            "as": f"{destino}_info"
                        }
                    }
                    pipeline.append(lookup_stage)
                    # $unwind
                    pipeline.append({"$unwind": f"${destino}_info"})
                    # $project si hay campos
                    if campos_proy:
                        project_stage = {"$project": {c: 1 for c in campos_proy}}
                        pipeline.append(project_stage)
                    return pipeline
                lines = [l.strip() for l in natural_text.split('\n') if l.strip()]
            if line.lower().startswith("desanidar "):
                path = line[len("desanidar "):].strip()
                # Soporte para preserveNullAndEmptyArrays (más robusto)
                preserve_empty = False
                if "con preservenullandemptyarrays" in path.lower() or "con preserveNullAndEmptyArrays" in path:
                    preserve_empty = True
                    # Extrae solo la ruta antes de la frase
                    path = path.split("con preserveNullAndEmptyArrays")[0].split("con preservenullandemptyarrays")[0].strip()
                if path and not path.lower().startswith("todos los niveles") and not path.lower().startswith("devices hasta"):
                    if preserve_empty:
                        pipeline.append({"$unwind": {"path": f"${path}", "preserveNullAndEmptyArrays": True}})
                    else:
                        pipeline.append({"$unwind": f"${path}"})
                    continue  # Ya procesado, saltar a la siguiente línea
            # --- FIN NUEVO ---
            if any(x in line.lower() for x in ["desanidar", "unwind", "expandir", "desglosa", "desglose"]):
                # Detectar si debe preservar arrays vacíos
                preserve_empty = any(x in line.lower() for x in [
                    "incluso si hay arrays vacíos", 
                    "aunque haya arrays vacíos",
                    "preserve null and empty arrays",
                    "con preservenullandemptyarrays"
                ])

                # Detectar si hay una frase 'hasta <ruta>'
                match = re.search(r"hasta ([\w\.]+)", line, re.IGNORECASE)
                if match:
                    ruta = match.group(1)
                    partes = ruta.split('.')
                    acumulado = []
                    for i, parte in enumerate(partes):
                        acumulado.append(parte)
                        path = '$' + '.'.join(acumulado)
                        if i == len(partes) - 1 and preserve_empty:
                            pipeline.append({"$unwind": {"path": path, "preserveNullAndEmptyArrays": True}})
                        else:
                            pipeline.append({"$unwind": path})
                    # NO break aquí, para permitir agregar más unwinds si hay variantes
                # Si no, usar lógica antigua para rutas conocidas
                if any(x in line.lower() for x in [
                    "devices hasta transactions",
                    "todos los niveles hasta transacciones",
                    "devices y servicepoints",
                    "desglosa todos los niveles"
                ]):
                    if preserve_empty:
                        pipeline.extend([
                            {"$unwind": "$Devices"},
                            {"$unwind": "$Devices.ServicePoints"},
                            {"$unwind": {"path": "$Devices.ServicePoints.ShipOutCycles", "preserveNullAndEmptyArrays": True}},
                            {"$unwind": {"path": "$Devices.ServicePoints.ShipOutCycles.Transactions", "preserveNullAndEmptyArrays": True}}
                        ])
                    else:
                        pipeline.extend([
                            {"$unwind": "$Devices"},
                            {"$unwind": "$Devices.ServicePoints"},
                            {"$unwind": "$Devices.ServicePoints.ShipOutCycles"},
                            {"$unwind": "$Devices.ServicePoints.ShipOutCycles.Transactions"}
                        ])
                    # NO break aquí, para permitir agregar más unwinds si hay variantes
        
        # 📊 PASO 2: Procesar $group (SmBoP - Construcción Bottom-up)
        for line in lines:
            if any(x in line.lower() for x in ["agrupar por", "agrupa por", "group by"]):
                group_stage = self._build_group_stage_from_text(line)
                if group_stage:
                    pipeline.append(group_stage)
                # NO break aquí para permitir múltiples $group
        
        # NUEVO: PASO 2.5: Procesar segundo $group (agrupación global)
        for line in lines:
            if any(x in line.lower() for x in ["agrupa todo", "agrupar globalmente", "luego agrupa todo", "agrupar por 0"]):
                # Segundo $group con _id: 0
                group_stage = {"$group": {"_id": 0}}
                
                # Sumas globales
                if "total de soles" in line.lower() or "total soles" in line.lower():
                    group_stage["$group"]["totalSoles"] = {"$sum": "$totalSoles"}
                if "total de dólares" in line.lower() or "total dólares" in line.lower():
                    group_stage["$group"]["totalDolares"] = {"$sum": "$totalDolares"}
                
                # Conteos condicionales de registros
                if "total de registros en soles" in line.lower() or "total registros soles" in line.lower():
                    group_stage["$group"]["totalRegSoles"] = {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "PEN"]},
                                1,
                                0
                            ]
                        }
                    }
                if "total de registros en dólares" in line.lower() or "total registros dólares" in line.lower():
                    group_stage["$group"]["totalRegDolares"] = {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "USD"]},
                                1,
                                0
                            ]
                        }
                    }
                
                pipeline.append(group_stage)
                break
        
    # PASO 3: Procesar $project (SmBoP - Parsers Especializados)
        for line in lines:
            # Detectar instrucciones de concatenación dinámicamente
            concat_patterns = [
                r'crear campo (\w+) que concatene: (.+)',
                r'crear campo (\w+) que sea la concatenaci[oó]n de (.+)',
                r'crear campo (\w+) que sea la concatenaci[oó]n entre (.+)',
                r'crear campo (\w+) concatenando (.+)',
                r'crear campo (\w+) que concatene (.+)'
            ]
            matched = False
            for pat in concat_patterns:
                reg_match = re.search(pat, line, re.IGNORECASE)
                if reg_match:
                    campo_reg = reg_match.group(1)
                    partes_raw = reg_match.group(2)
                    # Separar por coma, pero también soportar 'y', saltos de línea, etc.
                    partes = re.split(r',| y |\+|\band\b', partes_raw)
                    concat_expr = []
                    for parte in partes:
                        parte = parte.strip()
                        # Fecha con formato
                        fecha_match = re.search(r'el campo (\w+) convertido a formato ([%\w]+) usando los primeros (\d+) caracteres', parte, re.IGNORECASE)
                        if fecha_match:
                            campo_fecha = fecha_match.group(1)
                            fmt_fecha = fecha_match.group(2)
                            n_fecha = int(fecha_match.group(3))
                            fecha_expr = {
                                "$dateToString": {
                                    "date": {
                                        "$dateFromString": {
                                            "dateString": {"$substr": [f"${campo_fecha}", 0, n_fecha]}
                                        }
                                    },
                                    "format": fmt_fecha
                                }
                            }
                            concat_expr.append(fecha_expr)
                        elif parte.lower() in ['un salto de línea', 'otro salto de línea', 'otro salto de línea.']:
                            concat_expr.append("\n")
                        elif parte.lower() in ['un espacio', 'un espacio.']:
                            concat_expr.append(" ")
                        elif parte.startswith('"') and parte.endswith('"'):
                            concat_expr.append(parte.strip('"'))
                        elif parte.startswith("'") and parte.endswith("'"):
                            concat_expr.append(parte.strip("'"))
                        elif re.match(r'^[0-9]+$', parte):
                            concat_expr.append(parte)
                        else:
                            # Si parece un campo, anteponer $
                            if not parte.startswith('$') and parte.isidentifier():
                                concat_expr.append(f"${parte}")
                            else:
                                concat_expr.append(parte)
                    # Usar $addFields si ya hay un $project, si no, $project
                    add_to = None
                    for stage in pipeline:
                        if "$addFields" in stage:
                            add_to = stage["$addFields"]
                            break
                        if "$project" in stage:
                            add_to = stage["$project"]
                            break
                    if add_to is not None:
                        add_to[campo_reg] = {"$concat": concat_expr}
                    else:
                        pipeline.append({"$addFields": {campo_reg: {"$concat": concat_expr}}})
                    matched = True
                    break
            if matched:
                continue
            # 1. Mejorar: detectar campo destino explícito para fecha (mantener lógica existente)
            dateconv_match = re.search(r'crear campo (\w+) que convierta el campo (\w+) a formato ([%\w]+) usando los primeros (\d+) caracteres', line, re.IGNORECASE)
            if dateconv_match:
                campo_destino = dateconv_match.group(1)
                campo_origen = dateconv_match.group(2)
                fmt = dateconv_match.group(3)
                n = int(dateconv_match.group(4))
                expr = {
                    "$dateToString": {
                        "date": {
                            "$dateFromString": {
                                "dateString": {"$substr": [f"${campo_origen}", 0, n]}
                            }
                        },
                        "format": fmt
                    }
                }
                # Buscar si ya existe un $project en el pipeline
                project = None
                for stage in pipeline:
                    if "$project" in stage:
                        project = stage["$project"]
                        break
                if project is not None:
                    project[campo_destino] = expr
                else:
                    pipeline.append({"$project": {"_id": 0, campo_destino: expr}})
                continue
                # Detectar frases de substrCP avanzadas (varias variantes)
                if not processed:
                    substrcp_patterns = [
                        r'los caracteres de la posición (\d+) en adelante del (?:campo |id de )?(\w+)(?: usando \$substrCP)?',
                        r'extrae desde la posición (\d+) del (?:campo |id de )?(\w+)(?: usando \$substrCP)?',
                        r'a partir de la posición (\d+) del (?:campo |id de )?(\w+)(?: usando \$substrCP)?',
                        r'desde la posición (\d+) del (?:campo |id de )?(\w+)(?: usando \$substrCP)?',
                        r'los caracteres desde la posición (\d+) del (?:campo |id de )?(\w+)(?: usando \$substrCP)?',
                        r'los caracteres a partir de la posición (\d+) del (?:campo |id de )?(\w+)(?: usando \$substrCP)?'
                    ]
                    substrcp_match = None
                    for pattern in substrcp_patterns:
                        substrcp_match = re.search(pattern, line, re.IGNORECASE)
                        if substrcp_match:
                            break
                    if substrcp_match:
                        start = int(substrcp_match.group(1))
                        field = substrcp_match.group(2)
                        expr = {"$substrCP": [f"$_id.{field}", start, {"$strLenCP": f"$_id.{field}"}]}
                        project_exists = any("$project" in stage for stage in pipeline)
                        if project_exists:
                            for stage in pipeline:
                                if "$project" in stage:
                                    stage["$project"][field] = expr
                                    break
                        else:
                            pipeline.append({"$project": {field: expr}})
                        processed = True
                
                # Detectar frases de conversión de fecha con substr (caso individual)
                if not processed:
                    dateconv_match2 = re.search(r'convierte el campo (\w+) a formato ([%\w]+) usando los primeros (\d+) caracteres', line, re.IGNORECASE)
                    if dateconv_match2:
                        field = dateconv_match2.group(1)
                        fmt = dateconv_match2.group(2)
                        n = int(dateconv_match2.group(3))
                        expr = {
                            "$dateToString": {
                                "date": {
                                    "$dateFromString": {
                                        "dateString": {"$substr": [f"${field}", 0, n]}
                                    }
                                },
                                "format": fmt
                            }
                        }
                        project_exists = any("$project" in stage for stage in pipeline)
                        if project_exists:
                            for stage in pipeline:
                                if "$project" in stage:
                                    stage["$project"][field] = expr
                                    break
                        else:
                            pipeline.append({"$project": {field: expr}})
                        processed = True
                # Si no se procesó con ninguna de las reglas anteriores, usar el procesamiento simple
        # NUEVO: Procesar campos específicos mencionados en la consulta
        project_fields = []
        for line in lines:
            if "proyecta los siguientes campos" in line.lower():
                # Extraer campos de la lista
                fields_text = line.lower().split("campos:")[-1].strip()
                project_fields = [f.strip() for f in fields_text.split(",")]
                break
        
        # Si se encontraron campos específicos, crear $project
        if project_fields and not any("$project" in stage for stage in pipeline):
            project_stage = {"$project": {"_id": 0}}
            
            # Normalizar nombres de campos literales a canónicos para totalParteEntera y totalParteDecimal
            normalized_fields = []
            for field in project_fields:
                if field.startswith("totalParteEntera que sea el primer elemento"):
                    normalized_fields.append("totalparteentera")
                elif field.startswith("totalParteDecimal que sea el segundo elemento"):
                    normalized_fields.append("totalpartedecimal")
                else:
                    normalized_fields.append(field)
            project_fields = normalized_fields
            
            for field in project_fields:
                field = field.strip()
                if field in ["totalparteenterasoles", "total parte entera soles"]:
                    project_stage["$project"]["totalParteEnteraSoles"] = {
                        "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 0]
                    }
                elif field in ["totalpartedecimalsoles", "total parte decimal soles"]:
                    project_stage["$project"]["totalParteDecimalSoles"] = {
                        "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 1]}, "0"]}, "00"]
                    }
                elif field in ["totalparteenteradolares", "total parte entera dólares"]:
                    project_stage["$project"]["totalParteEnteraDolares"] = {
                        "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 0]
                    }
                elif field in ["totalpartedecimaldolares", "total parte decimal dólares"]:
                    project_stage["$project"]["totalParteDecimalDolares"] = {
                        "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 1]}, "0"]}, "00"]
                    }
                elif field in ["totalregsoles", "total registros soles"]:
                    project_stage["$project"]["totalRegSoles"] = "$totalRegSoles"
                elif field in ["totalregdolares", "total registros dólares"]:
                    project_stage["$project"]["totalRegDolares"] = "$totalRegDolares"
            
            pipeline.append(project_stage)
        
        # NUEVO: Procesar campo "reg" complejo si se menciona específicamente
        for line in lines:
            if "genera un campo reg" in line.lower() and "concatene" in line.lower():
                # Generar un segundo $project SOLO para reg, usando $substrCP y $strLenCP
                reg_project = {
                    "$project": {
                        "_id": 0,
                        "reg": {
                            "$concat": [
                                "9",
                                {
                                    "$substrCP": [
                                        {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]},
                                        {"$sum": [
                                            {"$strLenCP": {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]}},
                                            -15
                                        ]},
                                    ]
                                },
                                {
                                    "$substrCP": [
                                        {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]},
                                        {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}}, -15]},
                                        15
                                    ]
                                },
                                {
                                    "$substrCP": [
                                        {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]},
                                        {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}}, -15]},
                                        15
                                    ]
                                },
                                {
                                    "$substr": [
                                        {"$concat": ["0000000000000", "$totalParteEnteraSoles", {"$substr": ["$totalParteDecimalSoles", 0, 2]}]},
                                        {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}, -15]},
                                        {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}
                                    ]
                                },
                                {
                                    "$substr": [
                                        {"$concat": ["0000000000000", "$totalParteEnteraDolares", {"$substr": ["$totalParteDecimalDolares", 0, 2]}]},
                                        {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}, -15]},
                                        {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}
                                    ]
                                }
                            ]
                        }
                    }
                }
                pipeline.append(reg_project)
                break
        
        # NUEVO: Fallback - Si hay $group pero no $project, generar proyección automática
        has_group = any("$group" in stage for stage in pipeline)
        has_project = any("$project" in stage for stage in pipeline)
        
        if has_group and not has_project:
            # Generar $project automático con campos calculados
            project_stage = {"$project": {"_id": 0}}
            
            # Agregar campos de parte entera y decimal si existen totalSoles/totalDolares
            if any("totalSoles" in str(stage) for stage in pipeline):
                project_stage["$project"]["totalParteEnteraSoles"] = {
                    "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 0]
                }
                project_stage["$project"]["totalParteDecimalSoles"] = {
                    "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 1]}, "0"]}, "00"]
                }
            
            if any("totalDolares" in str(stage) for stage in pipeline):
                project_stage["$project"]["totalParteEnteraDolares"] = {
                    "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 0]
                }
                project_stage["$project"]["totalParteDecimalDolares"] = {
                    "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 1]}, "0"]}, "00"]
                }
            
            # Agregar campos de registros si existen
            if any("totalRegSoles" in str(stage) for stage in pipeline):
                project_stage["$project"]["totalRegSoles"] = "$totalRegSoles"
            if any("totalRegDolares" in str(stage) for stage in pipeline):
                project_stage["$project"]["totalRegDolares"] = "$totalRegDolares"
            
            # NUEVO: Generar campo "reg" complejo si se menciona
            if any("reg" in line.lower() for line in lines):
                # Campo reg con concatenación compleja
                project_stage["$project"]["reg"] = {
                    "$concat": [
                        "9",
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": {"$add": ["$totalRegSoles", "$totalRegDolares", 2]}}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteEnteraSoles"}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteDecimalSoles"}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteEnteraDolares"}]}, -15]},
                        {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteDecimalDolares"}]}, -15]}
                    ]
                }
            
            pipeline.append(project_stage)
        elif has_group and has_project:
            # Si ya existe $project pero no tiene los campos específicos, agregarlos
            for stage in pipeline:
                if "$project" in stage:
                    # Agregar campos de parte entera y decimal si existen totalSoles/totalDolares
                    if any("totalSoles" in str(s) for s in pipeline) and "totalParteEnteraSoles" not in stage["$project"]:
                        stage["$project"]["totalParteEnteraSoles"] = {
                            "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 0]
                        }
                        stage["$project"]["totalParteDecimalSoles"] = {
                            "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 1]}, "0"]}, "00"]
                        }
                    
                    if any("totalDolares" in str(s) for s in pipeline) and "totalParteEnteraDolares" not in stage["$project"]:
                        stage["$project"]["totalParteEnteraDolares"] = {
                            "$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 0]
                        }
                        stage["$project"]["totalParteDecimalDolares"] = {
                            "$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 1]}, "0"]}, "00"]
                        }
                    
                    # Agregar campos de registros si existen
                    if any("totalRegSoles" in str(s) for s in pipeline) and "totalRegSoles" not in stage["$project"]:
                        stage["$project"]["totalRegSoles"] = "$totalRegSoles"
                    if any("totalRegDolares" in str(s) for s in pipeline) and "totalRegDolares" not in stage["$project"]:
                        stage["$project"]["totalRegDolares"] = "$totalRegDolares"
                    
                    # NUEVO: Generar campo "reg" complejo si se menciona
                    if any("reg" in line.lower() for line in lines) and "reg" not in stage["$project"]:
                        stage["$project"]["reg"] = {
                            "$concat": [
                                "9",
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": {"$add": ["$totalRegSoles", "$totalRegDolares", 2]}}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteEnteraSoles"}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteDecimalSoles"}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteEnteraDolares"}]}, -15]},
                                {"$substr": [{"$concat": ["000000000000000", {"$toString": "$totalParteDecimalDolares"}]}, -15]}
                            ]
                        }
                    break
        
        # 📈 PASO 4: Procesar $sort (SmBoP - Orden Secuencial)
        for line in lines:
            if any(x in line.lower() for x in ["ordenar por", "sort by"]):
                sort_stage = self._build_sort_stage_from_text(line)
                if sort_stage:
                    pipeline.append(sort_stage)
                break
        
        # Al final de parse_natural_language, después de procesar todas las líneas:
        # 1. Fusionar todos los $project en uno solo
        projects = [stage["$project"] for stage in pipeline if "$project" in stage]
        if projects:
            merged_project = {}
            # Guardar expresiones reales de campos especiales
            expr_total_parte_entera = None
            expr_total_parte_decimal = None
            for proj in projects:
                for k, v in proj.items():
                    if k != '' and k not in merged_project:
                        # Normalizar nombre si termina con ' que sea _id' o similar
                        if k.endswith(' que sea _id'):
                            merged_project[k.replace(' que sea _id', '')] = v
                        elif k.startswith('totalParteEntera que sea el primer elemento'):
                            expr_total_parte_entera = v
                            merged_project['totalParteEntera'] = v
                        elif k.startswith('totalParteDecimal que sea el segundo elemento'):
                            expr_total_parte_decimal = v
                            merged_project['totalParteDecimal'] = v
                        else:
                            merged_project[k] = v
            # Reemplazo forzado de literales por expresiones MongoDB correctas para totalParteEntera y totalParteDecimal
            for k in list(merged_project.keys()):
                if isinstance(merged_project[k], str) and k.startswith('totalParteEntera'):
                    merged_project['totalParteEntera'] = {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$total"}}, "."]}, 0]}
                if isinstance(merged_project[k], str) and k.startswith('totalParteDecimal'):
                    merged_project['totalParteDecimal'] = {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$total"}}, "."]}, 1]}, "0"]}, "00"]}
            # Segunda pasada: normalizar referencias a campos intermedios SOLO si empiezan por $_id.
            for k, v in list(merged_project.items()):
                if isinstance(v, str) and v.endswith(' que sea _id'):
                    real_field = v.replace(' que sea _id', '').lstrip(' $.')
                    merged_project[k] = f"$_id.{real_field}"
                elif isinstance(v, str) and v.startswith('$_id.'):
                    # print(f"DEBUG: Antes de normalizar - k={k}, v='{v}'")
                    merged_project[k] = self._normalize_id_reference(v)
                elif isinstance(v, str) and v.startswith('totalParteEntera que sea el primer elemento'):
                    merged_project[k] = {"$arrayElemAt": [ {"$split": ["$total", "."]}, 0 ]}
                elif isinstance(v, str) and v.startswith('totalParteDecimal que sea el segundo elemento'):
                    merged_project[k] = {"$ifNull": [ {"$arrayElemAt": [ {"$split": ["$total", "."]}, 1 ]}, "00" ]}
            # Traducir alias en 'reg' si existe
            if 'reg' in merged_project and isinstance(merged_project['reg'], dict) and '$concat' in merged_project['reg']:
                new_concat = []
                for part in merged_project['reg']['$concat']:
                    if part == 'monedaCond':
                        new_concat.append({"$cond": [ {"$eq": ["$currencyCode", "PEN"] }, "00", "01"] })
                    elif part == 'deviceIdPad':
                        new_concat.append({"$substrCP": [ {"$concat": ["00000000000000000000", "$deviceId"] }, {"$sum": [ {"$strLenCP": {"$concat": ["00000000000000000000", "$deviceId"] } }, -20 ] }, 20 ] })
                    elif part == 'shipOutCodePad':
                        new_concat.append({"$substrCP": [ {"$concat": ["0000000000000000", {"$ifNull": ["$shipOutCode", "0"] }] }, {"$sum": [ {"$strLenCP": {"$concat": ["0000000000000000", {"$ifNull": ["$shipOutCode", "0"] }] } }, -16 ] }, 16 ] })
                    elif part == 'branchCodeCond':
                        new_concat.append({"$cond": [ {"$eq": ["$branchCode", "PE240"] }, "000", "001"] })
                    elif part == 'totalPad':
                        new_concat.append({"$substr": [ {"$concat": ["0000000000000", "$totalParteEntera", {"$substr": ["$totalParteDecimal", 0, 2] }] }, {"$sum": [ {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEntera", "00"] } }, -15 ] }, {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEntera", "00"] } }] })
                    elif part == 'confirmationCodePad':
                        new_concat.append({"$substrCP": [ {"$concat": [" ", "$confirmationCode"] }, {"$sum": [ {"$strLenCP": {"$concat": [" ", "$confirmationCode"] } }, -4 ] }, 4 ] })
                    elif part == 'un espacio.' or part == 'un espacio':
                        new_concat.append(" ")
                    else:
                        new_concat.append(part)
                merged_project['reg']['$concat'] = new_concat
            # Reemplazar todos los $project por uno solo
            pipeline = [stage for stage in pipeline if "$project" not in stage]
            pipeline.append({"$project": merged_project})
        # Eliminar campos vacíos ('': ...) y remanentes literales de instrucciones en $project
        for stage in pipeline:
            if "$project" in stage:
                # Eliminar claves vacías
                keys_to_remove = [k for k in stage["$project"] if k == '']
                for k in keys_to_remove:
                    del stage["$project"][k]
                # Eliminar claves que sean remanentes literales de instrucciones
                keys_to_remove = [k for k in stage["$project"] if any(
                    phrase in k.lower() for phrase in [
                        'que sea la concatenación',
                        'que sea el primer elemento',
                        'que sea el segundo elemento',
                        'que sea el substring',
                        'que convierta el campo',
                        'que sea _id',
                        'que sea el split',
                        'que sea el padding',
                        'que sea la condición',
                        'que sea la suma',
                        'que sea la proyección',
                        'que sea la conversión',
                        'que sea el campo',
                        'que sea la fecha',
                        'que sea el valor',
                        'que sea el total',
                        'que sea el monto',
                        'que sea la máscara',
                        'que sea la máscara de fecha',
                        'que sea la máscara de total',
                        'que sea la máscara de monto',
                        'que sea la máscara de campo',
                        'que sea la máscara de split',
                        'que sea la máscara de substring',
                        'que sea la máscara de padding',
                        'que sea la máscara de condición',
                        'que sea la máscara de suma',
                        'que sea la máscara de proyección',
                        'que sea la máscara de conversión',
                        'que sea la máscara de valor',
                        'que sea la máscara de _id',
                        'que sea la máscara de primer elemento',
                        'que sea la máscara de segundo elemento',
                        'que sea la máscara de concatenación',
                        'que sea la máscara de split',
                        'que sea la máscara de substring',
                        'que sea la máscara de padding',
                        'que sea la máscara de condición',
                        'que sea la máscara de suma',
                        'que sea la máscara de proyección',
                        'que sea la máscara de conversión',
                        'que sea la máscara de valor',
                        'que sea la máscara de _id',
                    ])]
                for k in keys_to_remove:
                    del stage["$project"][k]
        
        # Post-procesamiento dinámico para el campo reg en $project
        reg = None
        reg_stage = None
        for stage in pipeline:
            if "$project" in stage and "reg" in stage["$project"]:
                reg = stage["$project"]["reg"]
                reg_stage = stage
        # Solo procesar reg si fue asignado
        if reg is not None:
            # Asegurar que reg es un dict y tiene $concat
            if not (isinstance(reg, dict) and "$concat" in reg):
                # Si reg no es dict o no tiene $concat, inicializarlo correctamente
                if isinstance(reg, dict):
                    reg["$concat"] = []
                else:
                    reg = {"$concat": []}
                if reg_stage is not None:
                    reg_stage["$project"]["reg"] = reg
            # Ahora es seguro procesar
            if isinstance(reg, dict) and "$concat" in reg:
                new_concat = []
                for part in reg["$concat"]:
                    # Solo procesa strings, deja expresiones MongoDB tal cual
                    new_concat.append(part)
                reg["$concat"] = new_concat

                # Refuerzo global: filtra cualquier $project generado en cualquier parte del pipeline para que solo contenga campos válidos
                self._filtrar_project_global(pipeline, schema_fields)
                if isinstance(reg, dict) and "$concat" in reg:
                    new_concat = []
                    for part in reg["$concat"]:
                        # Solo procesa strings, deja expresiones MongoDB tal cual
                        if isinstance(part, str):
                            part_norm = self._normalize_concat_phrase(part)
                            expr = self._concat_map().get(part_norm, part)
                            new_concat.append(expr)
                        else:
                            new_concat.append(part)
                    reg["$concat"] = new_concat
        
                return pipeline
    
        
      

    def get_field_usage_ranking(self, queries: list) -> dict:
        """
        Analiza el ranking de campos usados en los pipelines generados.
        queries: lista de pipelines (list of dicts)
        return: dict con campo y frecuencia
        """
        from collections import Counter
        normaliza_campo_robusto = self.normaliza_campo_robusto
        fields = []
        for pipeline in queries:
            for stage in pipeline:
                if "$project" in stage:
                    fields.extend([normaliza_campo_robusto(f) for f in stage["$project"].keys()])
                if "$group" in stage and "_id" in stage["$group"]:
                    if isinstance(stage["$group"]["_id"], dict):
                        fields.extend([normaliza_campo_robusto(f) for f in stage["$group"]["_id"].keys()])
        return dict(Counter(fields))

    def calibrate_field_selection(self, queries: list, true_fields: list) -> float:
        """
        Calcula la precisión de selección de campos comparando los campos generados vs los esperados.
        queries: lista de pipelines (list of dicts)
        true_fields: lista de campos esperados
        return: accuracy
        """
        normaliza_campo_robusto = self.normaliza_campo_robusto
        total = 0
        correct = 0
        true_fields_norm = set([normaliza_campo_robusto(f) for f in true_fields])
        for pipeline in queries:
            used = set()
            for stage in pipeline:
                if "$project" in stage:
                    used.update([normaliza_campo_robusto(f) for f in stage["$project"].keys()])
                if "$group" in stage and "_id" in stage["$group"]:
                    if isinstance(stage["$group"]["_id"], dict):
                        used.update([normaliza_campo_robusto(f) for f in stage["$group"]["_id"].keys()])
            total += len(true_fields_norm)
            correct += len(true_fields_norm & used)
        return correct / total if total > 0 else 0.0

    def plot_learning_curve(self, X, y, model, cv=5):
        """
        Dibuja la curva de aprendizaje para el modelo dado.
        X: features, y: target, model: sklearn-like, cv: cross-validation folds
        """
        import matplotlib.pyplot as plt
        from sklearn.model_selection import learning_curve
        import numpy as np
        train_sizes, train_scores, test_scores = learning_curve(
            model, X, y, cv=cv, scoring='accuracy', n_jobs=-1,
            train_sizes=np.linspace(0.1, 1.0, 10)
        )
        train_mean = np.mean(train_scores, axis=1)
        test_mean = np.mean(test_scores, axis=1)
        plt.figure(figsize=(8,6))
        plt.plot(train_sizes, train_mean, 'o-', label='Train')
        plt.plot(train_sizes, test_mean, 'o-', label='Test')
        plt.xlabel('Training examples')
        plt.ylabel('Accuracy')
        plt.title('Curva de aprendizaje')
        plt.legend()
        plt.show()

    def generate_query(self, collection: str, natural_text: str, campos_esperados: set = None):
        """
        Genera una pipeline de MongoDB a partir de una consulta en lenguaje natural.
        Si se proporcionan campos_esperados, solo proyecta esos campos (filtro estricto) SOLO si la instrucción es simple.
        Devuelve el pipeline como lista de etapas (no string JSON).
        """
        # --- Si parse_natural_language soporta la instrucción, usar su resultado ---

        pipeline_nlp = self.parse_natural_language(natural_text, collection=collection, campos_esperados=campos_esperados)
        if pipeline_nlp and isinstance(pipeline_nlp, list) and len(pipeline_nlp) > 0:
            return pipeline_nlp
        # --- SOLUCIÓN ESPECIAL PARA LA INSTRUCCIÓN DE AGRUPACIÓN GLOBAL DE REGISTROS ---
        lower_text = natural_text.lower()

        # --- NUEVO: Conteo de clientes nuevos por mes ("¿Cuántos clientes nuevos hubo en octubre?") ---
        import calendar
        import datetime
        meses_es = [
            'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
            'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
        ]
        
        # --- MEJORA: Para ventas, si los campos ya son del tipo correcto, simplifica el pipeline ---
        if (
            ("venta" in lower_text or "ventas" in lower_text)
            and ("último mes" in lower_text or "ultimo mes" in lower_text or "mes pasado" in lower_text)
            and ("total" in lower_text or "suma" in lower_text or "cuánto" in lower_text or "cuanto" in lower_text)
        ):
            # Detectar si los campos ya son del tipo correcto
            is_fecha_date = False
            is_total_num = False
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                if 'fecha_venta' in schema.fields:
                    tipo_fecha = getattr(schema.fields['fecha_venta'], 'type', None)
                    if tipo_fecha and tipo_fecha.lower() in ['date', 'datetime', 'timestamp']:
                        is_fecha_date = True
                if 'total_venta' in schema.fields:
                    tipo_total = getattr(schema.fields['total_venta'], 'type', None)
                    if tipo_total and tipo_total.lower() in ['double', 'number', 'decimal', 'float', 'int', 'integer']:
                        is_total_num = True
            # Calcular fechas de inicio y fin del mes pasado
            import datetime
            today = datetime.datetime.now()
            first_day_this_month = today.replace(day=1)
            last_month_end = first_day_this_month - datetime.timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            fecha_ini = last_month_start.strftime("%Y-%m-%dT00:00:00Z")
            fecha_fin = last_month_end.strftime("%Y-%m-%dT23:59:59Z")
            if is_fecha_date and is_total_num:
                # Pipeline óptimo usando ISODate string directamente
                pipeline = [
                    {"$match": {"fecha_venta": {"$gte": fecha_ini, "$lte": fecha_fin}}},
                    {"$group": {"_id": None, "total_venta": {"$sum": "$total_venta"}}},
                    {"$project": {"total_venta": 1, "_id": 0}}
                ]
                return pipeline
            # Si no, usar el pipeline robusto (con conversiones)
            pipeline = [
                {"$addFields": {
                    "fecha_venta_date": {"$dateFromString": {"dateString": "$fecha_venta"}},
                    "total_venta_num": {"$toDouble": "$total_venta"}
                }},
                {"$match": {
                    "fecha_venta_date": {
                        "$gte": {"$dateFromString": {"dateString": fecha_ini}},
                        "$lte": {"$dateFromString": {"dateString": fecha_fin}}
                    }
                }},
                {"$group": {"_id": None, "total_venta": {"$sum": "$total_venta_num"}}},
                {"$project": {"total_venta": 1, "_id": 0}}
            ]
            return pipeline
        if (('cuántos' in lower_text or 'cuantos' in lower_text) and 'cliente' in lower_text and 'nuevo' in lower_text and any(m in lower_text for m in meses_es)):
            # Detectar el mes mencionado
            mes_idx = None
            for i, mes in enumerate(meses_es):
                if mes in lower_text:
                    mes_idx = i + 1
                    break
            if mes_idx:
                # Determinar año (por defecto, año actual)
                anio_actual = datetime.datetime.now().year
                # Buscar si se menciona un año explícito
                import re
                anio_match = re.search(r'(20\d{2})', lower_text)
                if anio_match:
                    anio = int(anio_match.group(1))
                else:
                    anio = anio_actual
                # Calcular rango de fechas ISO
                fecha_ini_iso = f"{anio}-{mes_idx:02d}-01T00:00:00Z"
                last_day = calendar.monthrange(anio, mes_idx)[1]
                fecha_fin_iso = f"{anio}-{mes_idx:02d}-{last_day:02d}T23:59:59Z"

                # Verificar si el campo es string o fecha (asumimos string por defecto)
                is_string = True
                if self.dataset_manager and collection in self.dataset_manager.schemas:
                    schema = self.dataset_manager.schemas[collection]
                    if 'fecha_registro' in schema.fields:
                        tipo = getattr(schema.fields['fecha_registro'], 'type', None)
                        if tipo and tipo.lower() in ['date', 'datetime', 'timestamp']:
                            is_string = False

                if is_string:
                    # Si es string, convertir en el $match usando $dateFromString directamente
                    pipeline = [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": [
                                        {"$dateFromString": {"dateString": "$fecha_registro"}},
                                        {"$toDate": fecha_ini_iso}
                                    ]},
                                    {"$lte": [
                                        {"$dateFromString": {"dateString": "$fecha_registro"}},
                                        {"$toDate": fecha_fin_iso}
                                    ]}
                                ]
                            }
                        }},
                        {"$count": "clientes_nuevos"}
                    ]
                else:
                    # Si ya es tipo fecha, filtra directamente
                    pipeline = [
                        {"$match": {
                            "fecha_registro": {
                                "$gte": {"$toDate": fecha_ini_iso},
                                "$lte": {"$toDate": fecha_fin_iso}
                            }
                        }},
                        {"$count": "clientes_nuevos"}
                    ]
                return pipeline
        if "luego agrupa todo y cuenta el total de registros" in lower_text:
            pipeline = [
                {"$group": {"_id": None, "total_registros": {"$sum": 1}}}
            ]
            return pipeline
        # Manejo especial para suma totalSoles y totalDolares y conteo por moneda
        if ("luego agrupa todo" in lower_text and "sum" in lower_text and "totalsoles" in lower_text and "totaldolares" in lower_text) or ("luego agrupa todo y suma totalsoles y totaldolares" in lower_text):
            pipeline = [
                {"$group": {
                    "_id": None,
                    "sumaTotalSoles": {"$sum": "$totalSoles"},
                    "sumaTotalDolares": {"$sum": "$totalDolares"},
                    "conteoSoles": {"$sum": {"$cond": [ {"$eq": ["$currencyCode", "PEN"]}, 1, 0 ]}},
                    "conteoDolares": {"$sum": {"$cond": [ {"$eq": ["$currencyCode", "USD"]}, 1, 0 ]}}
                }}
            ]
            return pipeline

        # Detectar si la instrucción es simple (sin palabras clave de etapas avanzadas)
        palabras_avanzadas = ["group", "agrupar", "addfields", "lookup", "unwind", "sort", "match", "limit", "filtra", "cuenta", "suma", "une", "desanidar", "ordenar", "buscar", "agrega", "crear", "concatena", "campo", "reg", "split", "join"]
        es_simple = not any(pal in natural_text.lower() for pal in palabras_avanzadas)

        # Si se pasan campos esperados, proyectar SIEMPRE todos los campos esperados (o equivalentes) en el $project, sin omitir ninguno
        if campos_esperados:
            campos_a_proyectar = list(campos_esperados)
            pipeline = []
            normaliza_campo_robusto = self.normaliza_campo_robusto
            from difflib import SequenceMatcher
            def buscar_equivalente(campo, schema, use_synonyms, threshold=None):
                campo_norm = normaliza_campo_robusto(campo)
                threshold = threshold if threshold is not None else getattr(self, 'threshold', 0.75)
                # Matching exacto y por sinónimos (bidireccional)
                for fname, fdef in schema.fields.items():
                    fname_norm = normaliza_campo_robusto(fname)
                    if fname_norm == campo_norm:
                        return fname
                    # Sinónimos directos
                    if use_synonyms:
                        syns = set(getattr(fdef, 'synonyms', []))
                        syns_norm = set([normaliza_campo_robusto(s) for s in syns])
                        if campo_norm in syns_norm:
                            return fname
                        # Bidireccional: si el campo tiene sinónimos y fname está en ellos
                        if hasattr(self, 'FIELD_SYNONYMS') and campo in self.FIELD_SYNONYMS:
                            if fname_norm in [normaliza_campo_robusto(s) for s in self.FIELD_SYNONYMS[campo]]:
                                return fname
                    # Fuzzy matching
                    ratio = SequenceMatcher(None, campo_norm, fname_norm).ratio()
                    if ratio >= threshold:
                        return fname
                    # Fuzzy con sinónimos
                    if use_synonyms:
                        for syn in getattr(fdef, 'synonyms', []):
                            syn_norm = normaliza_campo_robusto(syn)
                            ratio_syn = SequenceMatcher(None, campo_norm, syn_norm).ratio()
                            if ratio_syn >= threshold:
                                return fname
                        if hasattr(self, 'FIELD_SYNONYMS') and campo in self.FIELD_SYNONYMS:
                            for syn in self.FIELD_SYNONYMS[campo]:
                                syn_norm = normaliza_campo_robusto(syn)
                                ratio_syn = SequenceMatcher(None, fname_norm, syn_norm).ratio()
                                if ratio_syn >= threshold:
                                    return fname
                return None
            project_dict = {}
            campos_encontrados = 0
            if self.dataset_manager and collection in self.dataset_manager.schemas:
                schema = self.dataset_manager.schemas[collection]
                for campo in campos_a_proyectar:
                    equiv = buscar_equivalente(campo, schema, self.use_synonyms, self.threshold)
                    if equiv:
                        project_dict[equiv] = 1
                        campos_encontrados += 1
                    else:
                        project_dict[campo] = 1
            else:
                for campo in campos_a_proyectar:
                    project_dict[campo] = 1
            # Si no se encontró ningún campo equivalente ni existente, agregar un campo ficticio
            if len(project_dict) == 0 or all(k.startswith('campo_no_encontrado') or v != 1 for k, v in project_dict.items()):
                project_dict["campo_no_encontrado"] = 1
            project_stage = {"$project": project_dict}
            if es_simple:
                pipeline.append(project_stage)
                return pipeline
            # Si la instrucción es avanzada, agregar o fusionar $project en el pipeline generado
            pipeline = self.parse_natural_language(natural_text, collection=collection, campos_esperados=campos_esperados)
            if isinstance(pipeline, list):
                # Buscar si ya existe un $project
                project_found = False
                for stage in pipeline:
                    if isinstance(stage, dict) and "$project" in stage:
                        # Añadir todos los campos esperados (o equivalentes) al $project existente
                        for campo in campos_a_proyectar:
                            if campo not in stage["$project"]:
                                stage["$project"][campo] = 1
                        project_found = True
                        break
                if not project_found:
                    pipeline.append(project_stage)
            return pipeline

        # Si la instrucción NO es simple, delegar a parse_natural_language (que ya maneja casos avanzados y $project si corresponde)
        pipeline = self.parse_natural_language(natural_text, collection=collection, campos_esperados=campos_esperados)

        # --- MEJORA: Si se pasan campos_esperados, mapear nombres de salida en $project a los equivalentes esperados ---
        if campos_esperados and isinstance(pipeline, list):
            import unicodedata, re
            from difflib import SequenceMatcher
            def _normalize_for_compare(s):
                s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8').lower()
                s = re.sub(r'[^a-z0-9]', '', s)
                if len(s) > 3 and s.endswith('s'):
                    s = s[:-1]
                return s
            def _best_match(field, candidates, threshold=0.75):
                field_norm = _normalize_for_compare(field)
                best = None
                best_score = 0
                for c in candidates:
                    c_norm = _normalize_for_compare(c)
                    score = SequenceMatcher(None, field_norm, c_norm).ratio()
                    if score > best_score:
                        best = c
                        best_score = score
                if best_score >= threshold:
                    return best
                return None
            for stage in pipeline:
                if "$project" in stage:
                    new_proj = {}
                    for k, v in stage["$project"].items():
                        mapped = _best_match(k, campos_esperados)
                        new_proj[mapped if mapped else k] = v
                    stage["$project"] = new_proj

        # --- NUNCA retornar pipeline vacío: si pipeline es [] o None, intentar fallback para 'crear campo X' ---
        if not pipeline or (isinstance(pipeline, list) and len(pipeline) == 0):
            import re
            # Si la instrucción es 'crear campo ...', generar un $project con ese/estos campos
            match = re.match(r'crear campo ([\w, ]+)', natural_text.strip(), re.IGNORECASE)
            if match:
                fields = [f.strip() for f in match.group(1).split(',') if f.strip()]
                project_stage = {"$project": {}}
                for field in fields:
                    project_stage["$project"][field] = 1
                return [project_stage]
            return [{"$project": {"campo_no_encontrado": 1}}]

        # --- MEJORA: Fallback avanzado para extracción de campos si no se encontraron en etapas principales ---
        # Solo si se pasaron campos_esperados y no se encontraron en $project/$group/$addFields/$set/$match/$unwind/$lookup
        if campos_esperados and isinstance(pipeline, list):
            # Buscar si ya se extrajeron campos relevantes
            found = False
            main_ops = ["$project", "$group", "$addFields", "$set", "$match", "$unwind", "$lookup"]
            for stage in pipeline:
                for op in main_ops:
                    if op in stage and any(isinstance(stage[op], dict) and len(stage[op]) > 0 for op in stage if op in main_ops):
                        # Buscar si algún campo es relevante
                        for k in (stage[op].keys() if isinstance(stage[op], dict) else []):
                            for ce in campos_esperados:
                                if self._fuzzy_equiv_fallback(k, ce):
                                    found = True
                                    break
                            if found:
                                break
                    if found:
                        break
                if found:
                    break
            if not found:
                # Buscar en todas las keys de todos los stages (fallback total)
                extraidos = set()
                for ce in campos_esperados:
                    ce_norm = self.normaliza_campo_robusto(ce)
                    best_score = 0
                    best_key = None
                    for stage in pipeline:
                        for op, val in stage.items():
                            if isinstance(val, dict):
                                for k in val.keys():
                                    k_norm = self.normaliza_campo_robusto(k)
                                    score = self._fuzzy_score(ce_norm, k_norm)
                                    if score > best_score and score >= max(self.threshold, 0.75):
                                        best_score = score
                                        best_key = k
                    if best_key:
                        extraidos.add(best_key)
                # Si se encontraron campos relevantes, agregar un $project al final
                if extraidos:
                    pipeline.append({"$project": {k: 1 for k in extraidos}})
                else:
                    # --- NUEVO: Fallback extra, buscar en los valores de todos los stages (recursivo) ---
                    def find_keys_by_value_fuzzy(pipeline, expected_fields, threshold=0.75):
                        matches = set()
                        def search(obj, parent_key=None):
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    for ce in expected_fields:
                                        ce_norm = self.normaliza_campo_robusto(ce)
                                        # Buscar en el valor (si es str o simple)
                                        if isinstance(v, str):
                                            v_norm = self.normaliza_campo_robusto(v)
                                            score = self._fuzzy_score(ce_norm, v_norm)
                                            if score >= max(self.threshold, threshold):
                                                matches.add(k)
                                        # Buscar en listas de strings
                                        if isinstance(v, list):
                                            for item in v:
                                                if isinstance(item, str):
                                                    item_norm = self.normaliza_campo_robusto(item)
                                                    score = self._fuzzy_score(ce_norm, item_norm)
                                                    if score >= max(self.threshold, threshold):
                                                        matches.add(k)
                                    # Recursivo en subdicts/listas
                                    if isinstance(v, (dict, list)):
                                        search(v, k)
                            elif isinstance(obj, list):
                                for item in obj:
                                    search(item, parent_key)
                        search(pipeline)
                        return matches
                    matches = find_keys_by_value_fuzzy(pipeline, campos_esperados)
                    if matches:
                        pipeline.append({"$project": {k: 1 for k in matches}})
                    else:
                        pipeline.append({"$project": {"campo_no_encontrado": 1}})

        # --- POSTPROCESADO: Añadir campos calculados si la instrucción lo requiere ---
        # Solo para instrucciones que contienen 'crear campo dateMascara' o 'crear campo reg'
        lower_text = natural_text.lower()
        add_fields_stage = {}
        if 'crear campo datemascara' in lower_text:
            # Simulación: dateMascara = substr(date, 0, 8) (YYYYMMDD)
            add_fields_stage['dateMascara'] = {"$substr": ["$date", 0, 8]}
        if 'crear campo reg' in lower_text:
            # Simulación: reg = concat de partes según ejemplo
            add_fields_stage['reg'] = {"$concat": [
                "1", "002", {"$substr": ["$date", 0, 14]}, "00", "01", " ", "\n", "\n"
            ]}
        if add_fields_stage and isinstance(pipeline, list):
            pipeline.append({"$addFields": add_fields_stage})
        # --- REGLAS ESPECÍFICAS PARA INSTRUCCIONES FRECUENTES ---
        # 1. Conteo por ciudad y cliente
        if any(pal in lower_text for pal in ["cuántos", "cuantos"]) and "cliente" in lower_text and "ciudad" in lower_text:
            pipeline = [
                {"$group": {"_id": "$ciudad", "clientes": {"$sum": 1}}},
                {"$project": {"ciudad": "$_id", "clientes": 1, "_id": 0}}
            ]
            return pipeline

        # 2. Empleados por departamento
        if "empleado" in lower_text and "departamento" in lower_text:
            pipeline = [
                {"$group": {"_id": "$departamento", "empleados": {"$sum": 1}}},
                {"$project": {"departamento": "$_id", "empleados": 1, "_id": 0}}
            ]
            return pipeline

        # 3. Precio promedio de productos
        if ("precio promedio" in lower_text or "precio" in lower_text) and "producto" in lower_text:
            pipeline = [
                {"$group": {"_id": "$producto", "precio_promedio": {"$avg": "$precio"}}},
                {"$project": {"producto": "$_id", "precio": "$precio_promedio", "_id": 0}}
            ]
            return pipeline

        # 4. Casos generales: empleados, clientes, ciudad, etc.
        # Si la instrucción menciona empleados y no hay $group, agrupa por departamento si está presente
        if "empleado" in lower_text and "departamento" in lower_text and not any("$group" in stage for stage in pipeline if isinstance(stage, dict)):
            pipeline.append({"$group": {"_id": "$departamento", "empleados": {"$sum": 1}}})
            pipeline.append({"$project": {"departamento": "$_id", "empleados": 1, "_id": 0}})
            return pipeline
        # Si la instrucción menciona clientes y ciudad y no hay $group, agrupa por ciudad
        if "cliente" in lower_text and "ciudad" in lower_text and not any("$group" in stage for stage in pipeline if isinstance(stage, dict)):
            pipeline.append({"$group": {"_id": "$ciudad", "clientes": {"$sum": 1}}})
            pipeline.append({"$project": {"ciudad": "$_id", "clientes": 1, "_id": 0}})
            return pipeline
        # Si la instrucción menciona producto y precio, calcula precio promedio si no hay $group
        if "producto" in lower_text and "precio" in lower_text and not any("$group" in stage for stage in pipeline if isinstance(stage, dict)):
            pipeline.append({"$group": {"_id": "$producto", "precio_promedio": {"$avg": "$precio"}}})
            pipeline.append({"$project": {"producto": "$_id", "precio": "$precio_promedio", "_id": 0}})
            return pipeline

        # Si el resolutor de esquema está activado, intentar resolver y (opcionalmente) aplicar mapeo de campos
        try:
            if getattr(self, 'use_schema_resolver', False) and isinstance(pipeline, list):
                mapping = self.resolve_fields_for_pipeline(pipeline, collection=collection, threshold=getattr(self, 'threshold', None))
                applied = {k: v for k, v in mapping.items() if v.get('matched') is not None and v.get('score', 0) >= getattr(self, 'threshold', 0.75)}
                if applied:
                    from copy import deepcopy
                    mapped_pipeline = deepcopy(pipeline)
                    mapped_pipeline = self._apply_field_mapping(mapped_pipeline, applied)
                    # Guardar el mapping aplicado para trazabilidad sin alterar el retorno esperado
                    self._last_field_resolution = {'original': mapping, 'applied': applied}
                    pipeline = mapped_pipeline
                else:
                    self._last_field_resolution = {'original': mapping, 'applied': {}}
        except Exception as e:
            try:
                import logging
                logging.exception("Error al aplicar resolver de esquema: %s", e)
            except Exception:
                print(f"Error al aplicar resolver de esquema: {e}")

        return pipeline

    def _fuzzy_equiv_fallback(self, k, ce):
        # Normaliza y compara con sinónimos y fuzzy
        import unicodedata, re
        from difflib import SequenceMatcher
        def norm(x):
            x = unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode('utf-8').lower()
            x = re.sub(r'[^a-z0-9]', '', x)
            if len(x) > 3 and x.endswith('s'):
                x = x[:-1]
            return x
        k_norm = norm(k)
        ce_norm = norm(ce)
        if k_norm == ce_norm:
            return True
        # Sinónimos
        syns = []
        if hasattr(self, 'FIELD_SYNONYMS') and ce in self.FIELD_SYNONYMS:
            syns += [norm(s) for s in self.FIELD_SYNONYMS[ce]]
        if k in self.FIELD_SYNONYMS:
            syns += [norm(s) for s in self.FIELD_SYNONYMS[k]]
        if k_norm in syns or ce_norm in syns:
            return True
        # Fuzzy
        ratio = SequenceMatcher(None, k_norm, ce_norm).ratio()
        if ratio >= max(getattr(self, 'threshold', 0.75), 0.75):
            return True
        # Inclusión parcial
        if k_norm in ce_norm or ce_norm in k_norm:
            return True
        return False

    def _fuzzy_score(self, a, b):
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a, b).ratio()

    def analyze_pipeline(self, pipeline: list, collection: str = None, threshold: float = None) -> dict:
        """
        Analiza una pipeline generada y propone un mapeo fuzzy de los campos usados
        frente al esquema conocido (si `dataset_manager` está disponible).

        No modifica la pipeline. Retorna un dict con:
          - field_mappings: {campo_en_pipeline: {matched: campo_schema|None, score: 0.0}}
          - confidence: promedio de scores (0..1)

        Uso previsto: diagnóstico y trazabilidad desde el evaluator sin alterar
        el flujo de generación actual.
        """
        import unicodedata, re
        from difflib import SequenceMatcher

        # Preparar candidatos desde schema si está disponible
        schema_fields = []
        if hasattr(self, 'dataset_manager') and self.dataset_manager and collection and collection in self.dataset_manager.schemas:
            schema = self.dataset_manager.schemas[collection]
            # schema.fields puede ser un dict-like con field names as keys
            try:
                schema_fields = list(schema.fields.keys())
            except Exception:
                # Fallback: intentar iterar atributos de schema
                try:
                    schema_fields = [f.name for f in schema]
                except Exception:
                    schema_fields = []

        def normalize_name(s: str) -> str:
            s = str(s or '')
            s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8').lower()
            s = re.sub(r'[^a-z0-9]', '', s)
            return s

        def best_match(cand: str):
            cand_n = normalize_name(cand)
            best = None
            best_score = 0.0
            for f in schema_fields:
                score = SequenceMatcher(None, cand_n, normalize_name(f)).ratio()
                if score > best_score:
                    best_score = score
                    best = f
            return best, best_score

        # Extraer nombres de campos usados en la pipeline (heurística conservadora)
        used = set()

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    # keys in $project are likely field names
                    if k == '$project' and isinstance(v, dict):
                        for pk in v.keys():
                            used.add(pk)
                    # group _id puede ser string o dict
                    if k == '$group' and isinstance(v, dict) and '_id' in v:
                        _id = v['_id']
                        if isinstance(_id, str) and _id.startswith('$'):
                            used.add(_id.lstrip('$'))
                        elif isinstance(_id, dict):
                            for gid in _id.keys():
                                used.add(gid)
                    # match / sort / project may contain '$field' references
                    if isinstance(v, str) and v.startswith('$'):
                        used.add(v.lstrip('$'))
                    # recursive
                    walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(pipeline)

        # Para cada campo usado, proponer mejor match
        mappings = {}
        scores = []
        for field in sorted(used):
            matched, score = (None, 0.0)
            if schema_fields:
                matched, score = best_match(field)
            mappings[field] = {'matched': matched, 'score': float(score)}
            scores.append(score)

        confidence = float(sum(scores) / len(scores)) if scores else 0.0

        return {'field_mappings': mappings, 'confidence': confidence}


    def resolve_field(self, token: str, collection: str = None, threshold: float = None) -> dict:
        """
        Resolver un token de campo frente al esquema conocido usando sinónimos y fuzzy.
        Retorna: {'matched': campo_schema|None, 'score': float}
        """
        from difflib import SequenceMatcher

        threshold = threshold if threshold is not None else getattr(self, 'threshold', 0.75)
        if not hasattr(self, 'dataset_manager') or not self.dataset_manager or not collection or collection not in self.dataset_manager.schemas:
            return {'matched': None, 'score': 0.0}

        schema = self.dataset_manager.schemas[collection]
        tok_norm = self.normaliza_campo_robusto(token)
        best = None
        best_score = 0.0

        for fname, fdef in schema.fields.items():
            fname_norm = self.normaliza_campo_robusto(fname)
            if fname_norm == tok_norm:
                return {'matched': fname, 'score': 1.0}
            # fuzzy direct
            score = self._fuzzy_score(tok_norm, fname_norm)
            if score > best_score:
                best_score = score
                best = fname
            # synonyms on field definition
            for syn in getattr(fdef, 'synonyms', []):
                syn_norm = self.normaliza_campo_robusto(syn)
                s2 = self._fuzzy_score(tok_norm, syn_norm)
                if s2 > best_score:
                    best_score = s2
                    best = fname

        if best_score >= threshold:
            return {'matched': best, 'score': float(best_score)}
        return {'matched': None, 'score': float(best_score)}


    def resolve_fields_for_pipeline(self, pipeline: list, collection: str = None, threshold: float = None) -> dict:
        """
        Extrae nombres de campos desde una pipeline (heurística) y devuelve un mapping {campo_pipeline: {matched, score}}
        """
        used = set()

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    # keys que no son operadores ($) suelen ser campos
                    if isinstance(k, str) and not k.startswith('$'):
                        used.add(k)
                    # valores string con referencia $campo
                    if isinstance(v, str):
                        if v.startswith('$'):
                            used.add(v.lstrip('$'))
                    # recursivo
                    walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(pipeline)
        mapping = {}
        for u in sorted(used):
            mapping[u] = self.resolve_field(u, collection=collection, threshold=threshold)
        return mapping


    def _apply_field_mapping(self, pipeline: list, mapping: dict) -> list:
        """
        Aplica un mapping {orig: {matched,score}} a la pipeline, reemplazando claves y referencias tipo "$campo".
        Devuelve una copia transformada de la pipeline.
        """
        from copy import deepcopy

        def _repl(obj):
            if isinstance(obj, dict):
                new = {}
                for k, v in obj.items():
                    newk = mapping.get(k, {}).get('matched', k)
                    new[newk] = _repl(v)
                return new
            if isinstance(obj, list):
                return [_repl(x) for x in obj]
            if isinstance(obj, str):
                if obj.startswith('$'):
                    fld = obj.lstrip('$')
                    mapped = mapping.get(fld, {}).get('matched')
                    return f"${mapped}" if mapped else obj
                return obj
            return obj

        return _repl(deepcopy(pipeline))


        # 🧠 Aprendizaje de patrones (SmBoP)
        if self.dataset_manager:
            # Validar campos mencionados en la query
            self._validate_query_fields(natural_text, collection)

        # Detectar join explícito antes de generar el pipeline (más flexible)
        join_info = None
        join_patterns = [
            # une la colección empleados con la colección departamentos usando el campo departamento_id
            r'une (?:la colección )?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) con (?:la colección )?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) (?:usando|por|mediante|utilizando) (?:el campo |la clave |la columna |)?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+)',
            # join entre empleados y departamentos por departamento_id
            r'join (?:entre )?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) y ([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) (?:usando|por|mediante|utilizando) (?:el campo |la clave |la columna |)?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+)',
            # haz join de empleados y departamentos usando departamento_id
            r'haz join (?:de|entre)? ([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) y ([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) (?:usando|por|mediante|utilizando) (?:el campo |la clave |la columna |)?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+)',
            # empleados y departamentos por departamento_id
            r'([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) y ([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+) (?:usando|por|mediante|utilizando) (?:el campo |la clave |la columna |)?([\wáéíóúüñÁÉÍÓÚÜÑ\- ]+)'
        ]
        # Permitir errores menores de tipeo: si no hay match, buscar el join más aproximado
        if not join_info:
            import difflib
            palabras = re.findall(r'\b\w+\b', natural_text.lower())
            posibles = [p for p in palabras if len(p) > 3]
            # Si hay al menos 3 palabras, intentar buscar patrones join
            if 'join' in posibles or 'une' in posibles:
                # Buscar palabras que parezcan nombres de colección/campo
                candidates = [p for p in posibles if p not in ['join','une','con','usando','por','mediante','utilizando','campo','clave','columna','la','el','de','entre','y']]
                if len(candidates) >= 3:
                    local_collection, from_collection, local_field = candidates[:3]
                    join_info = {
                        "local_collection": local_collection,
                        "from_collection": from_collection,
                        "local_field": local_field
                    }
        for pat in join_patterns:
            join_match = re.search(pat, natural_text, re.IGNORECASE)
            if join_match:
                local_collection = join_match.group(1).strip()
                from_collection = join_match.group(2).strip()
                local_field = join_match.group(3).strip()
                join_info = {
                    "local_collection": local_collection,
                    "from_collection": from_collection,
                    "local_field": local_field
                }
                break

        # Generar pipeline
        pipeline = self.parse_natural_language(natural_text, collection=collection)

        # Si la instrucción es de join explícito y el pipeline NO contiene $lookup, agregarlo al inicio
        if join_info:
            has_lookup = any("$lookup" in stage for stage in pipeline)
            if not has_lookup:
                lookup_stage = {
                    "$lookup": {
                        "from": join_info["from_collection"],
                        "localField": join_info["local_field"],
                        "foreignField": join_info["local_field"],
                        "as": f"{join_info['from_collection']}_info"
                    }
                }
                unwind_stage = {"$unwind": f"${join_info['from_collection']}_info"}
                # Insertar al inicio del pipeline
                pipeline = [lookup_stage, unwind_stage] + pipeline

        # Si el pipeline sigue vacío, intentar fallback
        if not pipeline:
            normalized_text = self._normalize_text(natural_text)
            self._process_query_components(normalized_text)
            self._validate_pipeline()
            pipeline = self.pipeline

        # Si aún así el pipeline está vacío, fallback a Azure OpenAI LLM
            if not pipeline:
                campos_detectados = self._extract_fields_from_text(natural_text) if hasattr(self, '_extract_fields_from_text') else []
                campos_schema = []
                if self.dataset_manager and hasattr(self.dataset_manager, 'schemas') and collection in self.dataset_manager.schemas:
                    schema_obj = self.dataset_manager.schemas[collection]
                    if hasattr(schema_obj, 'fields'):
                        campos_schema = schema_obj.fields
                campos_schema_nombres = [f.name if hasattr(f, 'name') else f for f in campos_schema]
                ejemplo_query = f'db.getCollection("{collection}").aggregate([{{"$group": {{"_id": "$region", "totalVentas": {{"$sum": "$total_venta"}}}}}}])'
                error_msg = {
                    "error": "No se pudo generar la query automáticamente. Revisa los campos mencionados.",
                    "campos_detectados": campos_detectados,
                    "campos_esperados": campos_schema_nombres,
                    "ejemplo_query": ejemplo_query
                }
                if self.llm_engine:
                    suggestion = self.llm_engine.suggest_query_improvement(natural_text)
                    error_msg["llm_suggestion"] = suggestion
                return json.dumps(error_msg, ensure_ascii=False)
            # fallback si no hay LLM ni schema
            if not pipeline:
                pipeline = [{"$match": {}}]

        # --- LIMPIEZA DE CAMPOS Y FILTRO DE OPERADORES VÁLIDOS ---
        OPERADORES_VALIDOS = [
            "$match", "$group", "$project", "$sort", "$unwind", "$lookup", "$addFields",
            "$limit", "$skip", "$count", "$set", "$unset", "$replaceRoot", "$replaceWith", "$out", "$merge"
        ]

        def limpiar_campos(obj):
            if isinstance(obj, dict):
                return {k.replace(' ', '_'): limpiar_campos(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [limpiar_campos(item) for item in obj]
            else:
                return obj

        def es_operador_valido(etapa):
            if isinstance(etapa, dict) and len(etapa) == 1:
                return list(etapa.keys())[0] in OPERADORES_VALIDOS
            return False

        pipeline = limpiar_campos(pipeline)
        pipeline = [etapa for etapa in pipeline if es_operador_valido(etapa)]

        # Generar query final: devolver siempre la pipeline como lista de stages
        # Guardar también una representación en string para trazabilidad/registro
        generated_pipeline = pipeline
        try:
            generated_pipeline_str = json.dumps(pipeline, indent=2, ensure_ascii=False)
        except Exception:
            generated_pipeline_str = None

        # 🧠 Aprender del patrón generado (SmBoP) — pasar la representación en string si está disponible
        if self.dataset_manager:
            try:
                self.dataset_manager.learn_from_query(collection, natural_text, generated_pipeline_str if generated_pipeline_str is not None else generated_pipeline)
            except Exception:
                # No romper la generación si el dataset_manager falla al registrar
                pass

        # Guardar última pipeline generada para trazabilidad
        self._last_generated_pipeline = generated_pipeline
        self._last_generated_pipeline_str = generated_pipeline_str

        # Salvaguarda final: nunca retornar lista vacía, sino None
        if isinstance(generated_pipeline, list) and len(generated_pipeline) == 0:
            return None
        return generated_pipeline


    def _validate_query_fields(self, natural_text: str, collection: str):
        if not self.dataset_manager:
            return
        # Extraer posibles campos de la query
        words = natural_text.lower().split()
        unknown_fields = []
        for word in words:
            # Limpiar palabra
            clean_word = re.sub(r'[^\w]', '', word)
            if len(clean_word) > 2:  # Solo palabras significativas
                if not self._validate_field_with_dataset(clean_word, collection):
                    suggestions = self._suggest_fields_from_dataset(clean_word, collection)
                    if suggestions:
                        unknown_fields.append((clean_word, suggestions))
        # Mostrar sugerencias si hay campos desconocidos
        if unknown_fields:
            print(f"⚠️  Campos no encontrados en '{collection}':")
            for field, suggestions in unknown_fields:
                print(f"   '{field}' → Sugerencias: {suggestions}")


    def _normalize_text(self, text: str) -> str:
        replacements = {
            r'\bcreate\b': 'crear',
            r'\bfield\b': 'campo',
            r'\bformat\b': 'formato',
            r'\bconvert\b': 'convertir',
            r'\busing\b': 'usando',
            r'\bgroup by\b': 'agrupar por',
            r'\border by\b': 'ordenar por',
            r'\bunwind\b': 'desanidar',
            r'\bwith\b': 'con',
            r'\band\b': 'y'
        }
        normalized = text.lower()
        for pattern, repl in replacements.items():
            normalized = re.sub(pattern, repl, normalized)
        return normalized


    def _process_query_components(self, text: str):
        if self._is_complex_query(text):
            self._process_complex_query(text)
        else:
            # Procesamiento simple: si la instrucción es 'crear campo ...', genera un $project con esos campos
            match = re.match(r'crear campo ([\w, ]+)', text, re.IGNORECASE)
            if match:
                fields = [f.strip() for f in match.group(1).split(',') if f.strip()]
                project_stage = {"$project": {}}
                for field in fields:
                    project_stage["$project"][field] = 1
                self.pipeline.append(project_stage)


    def _is_complex_query(self, text: str) -> bool:
        complex_keywords = [
            'desanidar', 'agrupar por', 'transacciones',
            'dispositivos', 'servicepoints', 'shipoutcycles'
        ]
        return any(keyword in text.lower() for keyword in complex_keywords)

    def _process_complex_query(self, text: str):
        # 1. Desanidar estructuras
        if any(x in text.lower() for x in ["desanidar", "transacciones", "dispositivos"]):
            self.pipeline.extend([
                {"$unwind": "$Devices"},
                {"$unwind": "$Devices.ServicePoints"},
                {"$unwind": "$Devices.ServicePoints.ShipOutCycles"},
                {"$unwind": "$Devices.ServicePoints.ShipOutCycles.Transactions"}
            ])

        # 2. Agrupamiento
        group_stage = self._build_complex_group_stage(text)
        if group_stage:
            self.pipeline.append(group_stage)

        # 3. Proyecciones
        project_stages = self._build_complex_project_stages(text)
        self.pipeline.extend(project_stages)

        # 4. Ordenamiento
        if "ordenar por" in text.lower():
            sort_stage = self._build_sort_stage(text)
            if sort_stage:
                self.pipeline.append(sort_stage)

    def _build_complex_group_stage(self, text: str) -> Dict:
        # Extraer campos de agrupación desde la instrucción
        group_fields = []
        join_match = re.search(r'une la colección ([a-zA-Z0-9_]+) con la colección ([a-zA-Z0-9_]+) usando el campo ([a-zA-Z0-9_]+)', text, re.IGNORECASE)
        if join_match:
            group_fields = [self._normalize_field(f.strip()) for f in join_match.group(1).split(",") if f.strip()]
        else:
            # Si no se especifican, usar valores por defecto
            group_fields = ["date", "deviceId", "branchCode", "subChannelCode", "shipOutCode", "currencyCode", "confirmationCode"]

        _id = {}
        for field in group_fields:
            if "date" in field or "fecha" in field:
                fmt = "%Y%m%d%H%M%S"
                fmt_match = re.search(r'formato ([%\w]+)', text)
                if fmt_match:
                    fmt = fmt_match.group(1)
                _id["date"] = {"$dateToString": {"date": {"$dateFromString": {"dateString": {"$substr": ["$Date", 0, 19]}}}, "format": fmt}}
            else:
                # Mapear campos específicos a rutas completas
                if field.lower() in ["deviceid", "id de dispositivo", "dispositivo"]:
                    _id["deviceId"] = "$Devices.Id"
                elif field.lower() in ["branchcode", "código de sucursal", "sucursal"]:
                    _id["branchCode"] = "$Devices.BranchCode"
                elif field.lower() in ["subchannelcode", "subcanal"]:
                    _id["subChannelCode"] = "$Devices.ServicePoints.ShipOutCycles.SubChannelCode"
                elif field.lower() in ["shipoutcode", "código de envío", "envio", "código de ciclo de envío"]:
                    _id["shipOutCode"] = "$Devices.ServicePoints.ShipOutCycles.Code"
                elif field.lower() in ["currencycode", "moneda", "código de moneda"]:
                    _id["currencyCode"] = "$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode"
                elif field.lower() in ["confirmationcode", "código de confirmación", "confirmación"]:
                    _id["confirmationCode"] = "$Devices.ServicePoints.ShipOutCycles.Transactions.ConfirmationCode"
                else:
                    # Permitir rutas anidadas (Devices.Id, etc.)
                    if "." in field:
                        _id[field.split(".")[-1]] = f"${field}"
                    else:
                        _id[field] = f"${field}"

        # Suma si se menciona
        if self._find_operation(text, 'sum'):
            sum_field = "$total"
            # Detectar campo de suma si se menciona explícitamente
            sum_match = re.search(r'suma de ([\w\.]+)', text)
            if sum_match:
                sum_field = f"${sum_match.group(1)}"
            group_stage = {"$group": {"_id": _id, "total": {"$sum": sum_field}}}
        else:
            group_stage = {"$group": {"_id": _id}}

        return group_stage

    def _build_complex_project_stages(self, text: str) -> List[Dict]:
        stages = []
        text_lower = text.lower()

        # Definir lines para uso en la función
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # Extraer campos de proyección desde la instrucción
        project_fields = []
        match = re.search(r'(?:proyectar|project) ([\w,\. ]+)', text_lower)
        if match:
            project_fields = [self._normalize_field(f.strip()) for f in match.group(1).split(",") if f.strip()]
        
        # NUEVO: Detectar campos específicos mencionados en el texto
        if "totalparteenterasoles" in text_lower or "total parte entera soles" in text_lower:
            project_fields.append("totalparteenterasoles")
        if "totalpartedecimalsoles" in text_lower or "total parte decimal soles" in text_lower:
            project_fields.append("totalpartedecimalsoles")
        if "totalparteenteradolares" in text_lower or "total parte entera dólares" in text_lower:
            project_fields.append("totalparteenteradolares")
        if "totalpartedecimaldolares" in text_lower or "total parte decimal dólares" in text_lower:
            project_fields.append("totalpartedecimaldolares")
        if "totalregsoles" in text_lower or "total registros soles" in text_lower:
            project_fields.append("totalregsoles")
        if "totalregdolares" in text_lower or "total registros dólares" in text_lower:
            project_fields.append("totalregdolares")
        
        # Si no se especifican, usar algunos por defecto
        if not project_fields:
            project_fields = ["date", "deviceId", "branchCode", "currencyCode", "subChannelCode", "shipOutCode", "confirmationCode", "totalParteEntera", "totalParteDecimal", "reg", "dateMascara"]

        # Primera proyección
        stage1 = {"_id": 0}
        for field in project_fields:
            if field in ["date", "fecha"]:
                # Buscar formato
                fmt = "%Y%m%d%H%M%S"
                fmt_match = re.search(r'date.*formato ([%\w]+)', text_lower)
                if fmt_match:
                    fmt = fmt_match.group(1)
                stage1["date"] = {
                    "$dateToString": {
                        "date": {"$dateFromString": {"dateString": {"$substr": ["$Date", 0, 19]}}},
                        "format": fmt
                    }
                }
            elif field == "datemascara":
                fmt = "%Y%m%d"
                fmt_match = re.search(r'datemascara.*formato ([%\w]+)', text_lower)
                if fmt_match:
                    fmt = fmt_match.group(1)
                stage1["dateMascara"] = {
                    "$dateToString": {
                        "date": {"$dateFromString": {"dateString": {"$substr": ["$Date", 0, 19]}}},
                        "format": fmt
                    }
                }
            elif field == "reg":
                # NUEVO: Campo reg avanzado con concatenaciones complejas
                # Detectar si hay instrucciones específicas para reg
                reg_instructions = ""
                for line in lines:
                    if "reg" in line.lower() and ("concaten" in line.lower() or "concatene" in line.lower()):
                        reg_instructions = line.lower()
                        break
                
                if "9" in reg_instructions and ("total de registros" in reg_instructions or "totalreg" in reg_instructions):
                    # Reg complejo para el tercer output
                    stage1["reg"] = {
                        "$concat": [
                            "9",
                            {
                                "$substrCP": [
                                    {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]}}, -15]},
                                    15
                                ]
                            },
                            {
                                "$substrCP": [
                                    {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}}, -15]},
                                    15
                                ]
                            },
                            {
                                "$substrCP": [
                                    {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}}, -15]},
                                    15
                                ]
                            },
                            {
                                "$substr": [
                                    {"$concat": ["0000000000000", "$totalParteEnteraSoles", {"$substr": ["$totalParteDecimalSoles", 0, 2]}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}, -15]},
                                    {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}
                                ]
                            },
                            {
                                "$substr": [
                                    {"$concat": ["0000000000000", "$totalParteEnteraDolares", {"$substr": ["$totalParteDecimalDolares", 0, 2]}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}, -15]},
                                    {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}
                                ]
                            },
                            "\n",
                            "\n"
                        ]
                    }
                elif "5" in reg_instructions and ("deviceid" in reg_instructions or "device" in reg_instructions):
                    # Reg complejo para el segundo output
                    stage1["reg"] = {
                        "$concat": [
                            "5",
                            {"$cond": [{"$eq": ["$currencyCode", "PEN"]}, "00", "01"]},
                            "$date",
                            "00",
                            {
                                "$substrCP": [
                                    {"$concat": ["00000000000000000000", "$deviceId"]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["00000000000000000000", "$deviceId"]}}, -20]},
                                    20
                                ]
                            },
                            {
                                "$substrCP": [
                                    {"$concat": ["0000000000000000", {"$ifNull": ["$shipOutCode", "0"]}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["0000000000000000", {"$ifNull": ["$shipOutCode", "0"]}]}}, -16]},
                                    16
                                ]
                            },
                            {"$cond": [{"$eq": ["$branchCode", "PE240"]}, "000", "001"]},
                            {
                                "$substr": [
                                    {"$concat": ["0000000000000", "$totalParteEntera", {"$substr": ["$totalParteDecimal", 0, 2]}]},
                                    {"$sum": [{"$strLenCP": {"$concat": ["0000000000000", "$totalParteEntera", "00"]}}, -15]},
                                    {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEntera", "00"]}}
                                ]
                            },
                            " ",
                            {
                                "$substrCP": [
                                    {"$concat": [" ", "$confirmationCode"]},
                                    {"$sum": [{"$strLenCP": {"$concat": [" ", "$confirmationCode"]}}, -4]},
                                    4
                                ]
                            },
                            " "
                        ]
                    }
                else:
                    # Reg básico (plantilla original)
                    stage1["reg"] = {
                        "$concat": [
                            "1",
                            "002",
                            {"$dateToString": {"date": {"$dateFromString": {"dateString": {"$substr": ["$Date", 0, 19]}}}, "format": "%Y%m%d%H%M%S"}},
                            "00",
                            "01",
                            " ",
                            "\n",
                            "\n"
                        ]
                    }
            elif field == "totalparteentera":
                stage1["totalParteEntera"] = {"$arrayElemAt": [{"$split": [
                                {"$toString": {"$toDecimal": "$total"}},
                                "."
                            ]}, 0]}
            elif field == "totalpartedecimal":
                stage1["totalParteDecimal"] = {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$total"}}, "."]}, 1]}, "0"]}, "00"]}
            elif field == "totalparteenterasoles":
                stage1["totalParteEnteraSoles"] = {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 0]}
            elif field == "totalpartedecimalsoles":
                stage1["totalParteDecimalSoles"] = {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalSoles"}}, "."]}, 1]}, "0"]}, "00"]}
            elif field == "totalparteenteradolares":
                stage1["totalParteEnteraDolares"] = {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 0]}
            elif field == "totalpartedecimaldolares":
                stage1["totalParteDecimalDolares"] = {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$totalDolares"}}, "."]}, 1]}, "0"]}, "00"]}
            elif field == "totalregsoles":
                stage1["totalRegSoles"] = "$totalRegSoles"
            elif field == "totalregdolares":
                stage1["totalRegDolares"] = "$totalRegDolares"
            elif "." in field:
                # Permitir rutas anidadas
                stage1[field.split(".")[-1]] = f"${field}"
            else:
                # Proyección directa
                stage1[field] = f"$_id.{field}" if field in ["deviceId", "branchCode", "currencyCode", "subChannelCode", "shipOutCode", "confirmationCode"] else 1

        stages.append({"$project": stage1})
        
        # NUEVO: Segundo $project con _id: 0 si solo se necesita reg
        if "reg" in project_fields and len(project_fields) == 1:
            stage2 = {"_id": 0, "reg": stage1["reg"]}
            stages.append({"$project": stage2})
        
        return stages

    def _build_padded_expr(self, field: str, length: int, left_pad: bool = True) -> Dict:
        pad_char = "0" if left_pad else " "
        return {
            "$substrCP": [
                {"$concat": [pad_char * length, field] if left_pad else {"$concat": [field, " " * length]}},
                {"$subtract": [
                    {"$strLenCP": {"$concat": [pad_char * length, field] if left_pad else {"$concat": [field, " " * length]}}},
                    length
                ]},
                length
            ]
        }

    def _build_amount_expr(self) -> Dict:
        return {
            "$substr": [
                {"$concat": [
                    "0000000000000",
                    "$totalParteEntera",
                    {"$substr": ["$totalParteDecimal", 0, 2]}
                ]},
                {"$subtract": [{"$strLenCP": "0000000000000"}, 13]},
                15
            ]
        }

    def _build_date_expr(self, field: str) -> Dict:
        return {
            "$dateToString": {
                "format": "%Y%m%d%H%M%S",
                "date": {
                    "$dateFromString": {
                        "dateString": {"$substr": [f"${field}", 0, 19]},
                        "format": "%Y%m%d%H%M%S"
                    }
                }
            }
        }

    def _build_group_stage_from_text(self, text: str) -> Dict:

        group_fields = []
        match = re.search(r'(?:agrupar por|agrupa por|group by) ([\w\., y()%]+)', text, re.IGNORECASE)
        if match:
            group_fields = [self._normalize_field(f.strip()) for f in match.group(1).split(",") if f.strip() and len(f.strip()) < 30 and not f.lower().startswith("sumar el total")]
            # Asegura que currencyCode esté presente si se menciona
            if any("currencycode" in f.lower() for f in match.group(1).split(",")) and "currencyCode" not in group_fields:
                group_fields.append("currencyCode")
        else:
            group_fields = ["date", "deviceId", "branchCode", "subChannelCode", "shipOutCode", "currencyCode", "confirmationCode"]
        _id = {}
        for field in group_fields:
            if "date" in field or "fecha" in field:
                fmt = "%Y%m%d%H%M%S"
                fmt_match = re.search(r'formato ([%\w]+)', text)
                if fmt_match:
                    fmt = fmt_match.group(1)
                _id["date"] = {"$dateToString": {"date": {"$dateFromString": {"dateString": {"$substr": ["$Date", 0, 19]}}}, "format": fmt}}
            else:
                # Mapear campos específicos a rutas completas
                if field.lower() in ["deviceid", "id de dispositivo", "dispositivo"]:
                    _id["deviceId"] = "$Devices.Id"
                elif field.lower() in ["branchcode", "código de sucursal", "sucursal"]:
                    _id["branchCode"] = "$Devices.BranchCode"
                elif field.lower() in ["subchannelcode", "subcanal"]:
                    _id["subChannelCode"] = "$Devices.ServicePoints.ShipOutCycles.SubChannelCode"
                elif field.lower() in ["shipoutcode", "código de envío", "envio", "código de ciclo de envío"]:
                    _id["shipOutCode"] = "$Devices.ServicePoints.ShipOutCycles.Code"
                elif field.lower() in ["currencycode", "moneda", "código de moneda"]:
                    _id["currencyCode"] = "$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode"
                elif field.lower() in ["confirmationcode", "código de confirmación", "confirmación"]:
                    _id["confirmationCode"] = "$Devices.ServicePoints.ShipOutCycles.Transactions.ConfirmationCode"
                else:
                    # Permitir rutas anidadas (Devices.Id, etc.)
                    if "." in field:
                        _id[field.split(".")[-1]] = f"${field}"
                    else:
                        _id[field] = f"${field}"

        # Normalizar nombres de campos para el $group, especialmente confirmationCode
        normalized_group_fields = []
        for field in group_fields:
            f_norm = field.lower().replace(' ', '').replace('_', '')
            if f_norm in ["confirmationcode", "códigodeconfirmación", "confirmación"]:
                normalized_group_fields.append("confirmationCode")
            else:
                normalized_group_fields.append(field)
        group_fields = normalized_group_fields

        # NUEVO: Acumuladores condicionales para soles y dólares
        acumuladores = {}
        text_norm = text.replace('\n', ' ').replace('\r', ' ')
        
        # Detectar frases de suma condicional
        sum_cond_match = re.search(
            r'suma(?:r)?(?: el)? total (?:de )?devices\.servicepoints\.shipoutcycles\.transactions\.total en (soles|d[oó]lares) y en (soles|d[oó]lares) seg[uú]n el c[oó]digo de moneda',
            text_norm, re.IGNORECASE)
        
        if sum_cond_match:
            campo1, campo2 = sum_cond_match.groups()
            moneda_map = {"soles": "PEN", "dolares": "USD", "dólares": "USD"}
            val1 = moneda_map.get(campo1.strip().lower(), campo1.upper())
            val2 = moneda_map.get(campo2.strip().lower(), campo2.upper())
            total_path = "Devices.ServicePoints.ShipOutCycles.Transactions.Total"
            currency_path = "$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode"
            acumuladores["totalSoles"] = {
                "$sum": {
                    "$cond": [
                        {"$eq": [currency_path, val1]},
                        f"${total_path}",
                        0
                    ]
                }
            }
            acumuladores["totalDolares"] = {
                "$sum": {
                    "$cond": [
                        {"$eq": [currency_path, val2]},
                        f"${total_path}",
                        0
                    ]
                }
            }
        else:
            # Detectar suma condicional más simple
            if "suma el total de transacciones en soles y en dólares según el código de moneda" in text_norm.lower():
                acumuladores["totalSoles"] = {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "PEN"]},
                            "$Devices.ServicePoints.ShipOutCycles.Transactions.Total",
                            0
                        ]
                    }
                }
                acumuladores["totalDolares"] = {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode", "USD"]},
                            "$Devices.ServicePoints.ShipOutCycles.Transactions.Total",
                            0
                        ]
                    }
                }
            elif self._find_operation(text, 'sum'):
                sum_field = "$total"
                # Detectar campo de suma si se menciona explícitamente
                sum_match = re.search(r'suma de ([\w\.]+)', text, re.IGNORECASE)
                if sum_match:
                    sum_field = f"${sum_match.group(1)}"
                acumuladores["total"] = {"$sum": sum_field}

        group_stage = {"$group": {"_id": _id}}
        if acumuladores:
            group_stage["$group"].update(acumuladores)

        return group_stage

    def _build_sort_stage_from_text(self, text: str) -> Dict:

        if "ordenar por" in text.lower():
            # Extraer campos después de "ordenar por"
            sort_part = text.lower().split("ordenar por")[1].strip()
            # Separar por comas y "y"
            fields = re.split(r'[, y]+', sort_part)
            sort_fields = {}
            for field in fields:
                field = field.strip()
                if field == "deviceid" or field == "dispositivo":
                    sort_fields["deviceId"] = 1
                elif field == "shipoutcode" or field == "envio":
                    sort_fields["shipOutCode"] = 1
                elif field == "subchannelcode" or field == "subcanal":
                    sort_fields["subChannelCode"] = 1
                elif field == "currencycode" or field == "moneda":
                    sort_fields["currencyCode"] = 1
                elif field == "date" or field == "fecha":
                    sort_fields["date"] = 1
                elif field == "branchcode" or field == "sucursal":
                    sort_fields["branchCode"] = 1
                elif field == "confirmationcode":
                    sort_fields["confirmationCode"] = 1
            return {"$sort": sort_fields} if sort_fields else None
        return None

    def _build_sort_stage(self, text: str) -> Dict:
        if "ordenar por" in text.lower():
            fields = text.lower().split("ordenar por")[1].strip().split(" y ")
            sort_fields = {}
            for field in fields:
                field = field.strip()
                if field == "dispositivo":
                    sort_fields["deviceId"] = 1
                elif field == "envio":
                    sort_fields["shipOutCode"] = 1
                elif field == "subcanal":
                    sort_fields["subChannelCode"] = 1
                elif field == "moneda":
                    sort_fields["currencyCode"] = 1
                elif field == "fecha":
                    sort_fields["date"] = 1
                elif field == "sucursal":
                    sort_fields["branchCode"] = 1
            return {"$sort": sort_fields} if sort_fields else None
        return None

    def _extract_substrcp_operation_for_field(self, text: str, field: str, source_field: str = None, start: str = None) -> Optional[Dict]:

        # Permite pasar source_field y start directamente
        if source_field and start:
            if source_field.startswith('_id.'):
                source_field_expr = f"${source_field}"
            else:
                source_field_expr = f"${source_field}"
            return {
                "$substrCP": [
                    source_field_expr,
                    int(start),
                    {"$strLenCP": source_field_expr}
                ]
            }
        # Fallback: buscar en el texto
        import re
        pattern = r'crear campo ([\w]+) que sea el substring de ([\w\.]+) desde la posición (\d+) hasta el largo del campo'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, source_field, start = match.groups()
            if source_field.startswith('_id.'):
                source_field_expr = f"${source_field}"
            else:
                source_field_expr = f"${source_field}"
            return {
                "$substrCP": [
                    source_field_expr,
                    int(start),
                    {"$strLenCP": source_field_expr}
                ]
            }
        return None

    def _extract_ifnull_operation_for_field(self, text: str, field: str) -> Optional[Dict]:
        import re
        pattern = r'crear campo (\w+) que sea (\w+) o "([^"]*)" si es nulo'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, source_field, default_value = match.groups()
            # Maneja campos con _id.
            if source_field.startswith('_id.'):
                source_field = source_field
            else:
                source_field = f"${source_field}"
            return {
                "$ifNull": [source_field, default_value]
            }
        return None

    def _extract_cond_operation_for_field(self, text: str, field: str) -> Optional[Dict]:
        import re
        pattern = r'crear campo (\w+) que sea "([^"]*)" si (\w+) es "([^"]*)" y "([^"]*)" en otro caso'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, true_val, cond_field, cond_val, false_val = match.groups()
            # Maneja campos con _id.
            if cond_field.startswith('_id.'):
                cond_field = cond_field
            else:
                cond_field = f"${cond_field}"
            return {
                "$cond": [
                    {"$eq": [cond_field, cond_val]},
                    true_val,
                    false_val
                ]
            }
        return None

    def _extract_padding_operation_for_field(self, text: str, field: str) -> Optional[Dict]:
        # Ejemplo: crear campo deviceIdPad con padding izquierda 20 de deviceId
        import re
        pattern = r'crear campo (\w+) con padding izquierda (\d+) de (\w+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, pad_len, source_field = match.groups()
            pad_len = int(pad_len)
            pad_str = "0" * pad_len
            # Maneja campos con _id.
            if source_field.startswith('_id.'):
                source_field = source_field
            else:
                source_field = f"${source_field}"
            return {
                "$substrCP": [
                    {"$concat": [pad_str, source_field]},
                    {"$subtract": [
                        {"$strLenCP": {"$concat": [pad_str, source_field]}},
                        pad_len
                    ]},
                    pad_len
                ]
            }
        return None
    
    def _normalize_concat_phrase(self, phrase: str) -> str:

         import unicodedata
         s = phrase.lower().strip()
         s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
         s = s.replace('.', '').replace('"', '').replace("'", '')
         import re
         s = re.sub(r'\s+', ' ', s)
         return s

    def _concat_map(self):
        # Centraliza el mapeo de frases a expresiones MongoDB
        return {
            "el total de registros con padding": {
                "$substrCP": [
                    {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]},
                    {"$sum": [
                        {"$strLenCP": {"$concat": ["000000000000000", {"$toString": {"$sum": ["$totalRegSoles", "$totalRegDolares", 2]}}]}},
                        -15
                    ]},
                    15
                ]
            },
            "el total de registros en soles con padding": {
                "$substrCP": [
                    {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]},
                    {"$sum": [
                        {"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegSoles"}]}},
                        -15
                    ]},
                    15
                ]
            },
            "el total de registros en dolares con padding": {
                "$substrCP": [
                    {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]},
                    {"$sum": [
                        {"$strLenCP": {"$concat": ["000000000000000", {"$toString": "$totalRegDolares"}]}},
                        -15
                    ]},
                    15
                ]
            },
            "el monto en soles con padding": {
                "$substr": [
                    {"$concat": ["0000000000000", "$totalParteEnteraSoles", {"$substr": ["$totalParteDecimalSoles", 0, 2]}]},
                    {"$sum": [
                        {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}},
                        -15
                    ]},
                    {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraSoles", "00"]}}
                ]
            },
            "el monto en dolares con padding": {
                "$substr": [
                    {"$concat": ["0000000000000", "$totalParteEnteraDolares", {"$substr": ["$totalParteDecimalDolares", 0, 2]}]},
                    {"$sum": [
                        {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}},
                        -15
                    ]},
                    {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEnteraDolares", "00"]}}
                ]
            }
        }

    def _extract_advanced_concat_for_field(self, text: str, field: str, context_fields: dict) -> Optional[Dict]:
        import re
        pattern = r'crear campo (\w+) concatene: ([^\n]+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, concat_args = match.groups()
            parts = [p.strip() for p in re.findall(r'"[^"]*"|[^,]+', concat_args)]
            concat_map = self._concat_map()
            concat_list = []
            for part in parts:
                part_clean = part.strip('"').strip()
                part_norm = self._normalize_concat_phrase(part_clean)
                # Manejo robusto de variantes
                if part.startswith('"') and part.endswith('"'):
                    concat_list.append(part.strip('"'))
                elif part_norm in concat_map:
                    concat_list.append(concat_map[part_norm])
                elif "salto de linea" in part_norm:
                    concat_list.append("\n")
                elif part_norm == "un" or part_norm == "otro":
                    continue  # Ignora 'un' y 'otro' si están solos por saltos de línea
                elif part in context_fields:
                    concat_list.append(f"${part}")
                elif part.startswith('_id.'):
                    concat_list.append(part)
                else:
                    concat_list.append(f"${part_norm}")
            return {"$concat": concat_list}
        return None

    def _extract_date_conversion_for_field(self, text: str, field: str) -> Optional[Dict]:
        # Busca cualquier frase 'que convierta el campo <algo> a formato <formato>'
        pattern = r'que convierta el campo (\w+) a formato (\w+)(?: usando los primeros (\d+) caracteres)?'
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            date_field, fmt, substr_len = match.groups()
            return self._create_date_conversion(date_field, fmt, substr_len)
        return None

    def _create_date_conversion(self, date_field: str, fmt: str, substr_len: Optional[str]) -> Dict:

        fmt_key = fmt.upper()
        if fmt_key not in self.date_formats:
            return 1

        # 🔬 Determinar el campo correcto según el contexto (Bridging the Gap - Manejo de Contexto)
        if date_field.lower() == "date":
            # Si es "date", usar $_id.date (del grupo)
            field_expr = "$_id.date"
        else:
            # Para otros campos, usar el campo directamente
            field_expr = f"${date_field}"

        # 🧠 Solo usa substr si el usuario lo pide (SmBoP - Construcción Condicional)
        if substr_len:
            date_expr = {"$substr": [field_expr, 0, int(substr_len)]}
        else:
            date_expr = field_expr

        return {
            "$dateToString": {
                "format": self.date_formats[fmt_key],
                "date": {"$dateFromString": {
                    "dateString": date_expr
                }}
            }
        }

    def _extract_fields_to_create(self, text: str) -> list:
        import re
        phrases = re.split(r'(?=crear campo )', text, flags=re.IGNORECASE)
        fields = []
        for phrase in phrases:
            phrase = phrase.strip()
            if not phrase.lower().startswith('crear campo'):
                continue
            # totalParteEntera: primer elemento del split del total por punto
            match_total_entera = re.search(r'crear campo\s+(totalParteEntera) que sea el primer elemento del split del total por punto', phrase, re.IGNORECASE)
            if match_total_entera:
                fields.append(('totalParteEntera', True, phrase, None, None, None, False))
                continue
            # totalParteDecimal: segundo elemento del split del total por punto o "00" si es nulo
            match_total_decimal = re.search(r'crear campo\s+(totalParteDecimal) que sea el segundo elemento del split del total por punto o "([^"]*)" si es nulo', phrase, re.IGNORECASE)
            if match_total_decimal:
                default_val = match_total_decimal.group(2)
                fields.append(('totalParteDecimal', True, phrase, None, None, default_val, False))
                continue
            # amountPad: substring de la concatenación de ...
            match_amount_pad = re.search(r'crear campo\s+(amountPad) que sea el substring de la concatenación de "0000000000000", totalParteEntera y totalParteDecimal desde la posición calculada', phrase, re.IGNORECASE)
            if match_amount_pad:
                fields.append(('amountPad', True, phrase, None, None, None, False))
                continue
            # Detecta substring primero para extraer nombre limpio y source dinámicamente
            match_substr = re.search(r'crear campo\s+([\w]+) que sea el substring de ([\w\.]+) desde la posición (\d+) hasta el largo del campo', phrase, re.IGNORECASE)
            if match_substr:
                field_name, source_field, start = match_substr.groups()
                fields.append((field_name, True, phrase, source_field, None, start, False))
                continue
            # Detecta operaciones con $arrayElemAt y $split
            match_arrayelem = re.search(r'crear campo\s+([\w]+) que sea el (primer|segundo) elemento del split de (\w+) por (\w+)', phrase, re.IGNORECASE)
            if match_arrayelem:
                field_name, position, source_field, split_char = match_arrayelem.groups()
                fields.append((field_name, True, phrase, source_field, position, split_char, False))
                continue
            # Detecta operaciones con $arrayElemAt y $split con ifNull
            match_arrayelem_ifnull = re.search(r'crear campo\s+([\w]+) que sea el (primer|segundo) elemento del split de (\w+) por (\w+) o "([^"]*)" si es nulo', phrase, re.IGNORECASE)
            if match_arrayelem_ifnull:
                field_name, position, source_field, split_char, default_val = match_arrayelem_ifnull.groups()
                fields.append((field_name, True, phrase, source_field, position, split_char, False))
                continue
            # Detecta operaciones con $substr complejas
            match_substr_complex = re.search(r'crear campo\s+([\w]+) que sea el substring de la concatenación de ([^d]+) desde la posición calculada', phrase, re.IGNORECASE)
            if match_substr_complex:
                field_name, concat_parts = match_substr_complex.groups()
                fields.append((field_name, True, phrase, concat_parts, None, None, False))
                continue
            # Detecta operaciones de concatenación complejas
            match_concat_complex = re.search(r'crear campo\s+([\w]+) concatenando ([^f]+)', phrase, re.IGNORECASE)
            if match_concat_complex:
                field_name, concat_parts = match_concat_complex.groups()
                fields.append((field_name, True, phrase, concat_parts, None, None, True))
                continue
            # Extrae el nombre limpio del campo (primera palabra después de 'crear campo')
            match_field = re.match(r'crear campo\s+([\w]+)', phrase, re.IGNORECASE)
            field = match_field.group(1) if match_field else None
            if not field:
                continue
            # Detecta campos con formato especial de fecha
            match_special = re.search(r'crear campo\s+[\w]+(?:\s+con formato)? que convierta el campo (\w+) a formato (\w+)(?: usando los primeros (\d+) caracteres)?', phrase, re.IGNORECASE)
            if match_special:
                date_field, fmt, substr_len = match_special.groups()
                fields.append((field, True, phrase, date_field, fmt, substr_len, False))
                continue
            # Detecta campos con formato concat
            match_concat = re.search(r'crear campo\s+[\w]+\s+con formato concat\((.+)\)', phrase, re.IGNORECASE)
            if match_concat:
                fields.append((field, True, phrase, None, None, None, True))
                continue
            # Detecta padding avanzado
            match_advpad = re.search(r'crear campo\s+[\w]+\s+con padding izquierda (\d+) de (\w+) usando \$sum y \$strLenCP', phrase, re.IGNORECASE)
            if match_advpad:
                fields.append((field, True, phrase, None, None, None, False))
                continue
            # Detecta padding simple
            match_pad = re.search(r'crear campo\s+[\w]+\s+con padding izquierda (\d+) de (\w+)', phrase, re.IGNORECASE)
            if match_pad:
                fields.append((field, True, phrase, None, None, None, False))
                continue
            # Detecta ifNull
            match_ifnull = re.search(r'crear campo\s+[\w]+ que sea (\S+) o "([^"]*)" si es nulo', phrase, re.IGNORECASE)
            if match_ifnull:
                fields.append((field, True, phrase, None, None, None, False))
                continue
            # Detecta cond
        """
        🎯 IMPLEMENTACIÓN DE PRINCIPIOS DE PAPERS ACADÉMICOS

        📚 PAPERS IMPLEMENTADOS:
        1. "Bridging the Gap: Enabling Natural Language Queries for NoSQL Databases through Text-to-NoSQL Translation"
        2. "SmBoP: Semi-autoregressive Bottom-up Semantic Parsing"

        🔬 PRINCIPIOS DE "BRIDGING THE GAP":
        - Normalización de texto natural a operadores NoSQL
        - Mapeo de sinónimos y variaciones lingüísticas
        - Traducción de frases especiales a rutas de datos
        - Validación semántica de campos y operaciones
        - Manejo de contexto y referencias anidadas

        🧠 PRINCIPIOS DE "SMBOP":
        - Parsing semi-autoregresivo de instrucciones secuenciales
        - Construcción bottom-up de expresiones complejas
        - Parsers especializados por tipo de operación
        - Acumulación progresiva de pipeline de agregación
        - Manejo de dependencias entre operaciones

        🚀 CARACTERÍSTICAS IMPLEMENTADAS:
        - Soporte completo para operaciones MongoDB complejas
        - Generación dinámica de pipelines de agregación
        - Manejo de campos anidados y referencias
        - Operaciones avanzadas: $substrCP, $ifNull, $cond, $arrayElemAt, $split
        - Concatenaciones complejas con formato de fecha
        - Padding dinámico con $sum y $strLenCP
        """

        # if match_cond:  # Eliminado porque match_cond no está definido
        #     fields.append((field, True, phrase, None, None, None, False))
        #     # continue eliminado porque no está en un bucle
        #     # Campos simples (pueden ser varios separados por coma) - solo si no se procesó como especial
        if not any([
                re.search(r'crear campo\s+[\w]+(?:\s+con formato)? que convierta el campo (\w+) a formato (\w+)(?: usando los primeros (\d+) caracteres)?', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+\s+con formato concat\((.+)\)', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+\s+con padding izquierda (\d+) de (\w+) usando \$sum y \$strLenCP', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+\s+con padding izquierda (\d+) de (\w+)', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea el substring de ([\w\.]+) desde la posición (\d+) hasta el largo del campo', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea (\S+) o "([^"]*)" si es nulo', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea "([^"]*)" si (\w+) es "([^"]*)" y "([^"]*)" en otro caso', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea el (primer|segundo) elemento del split de (\w+) por (\w+)', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea el (primer|segundo) elemento del split de (\w+) por (\w+) o "([^"]*)" si es nulo', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ que sea el substring de la concatenación de ([^d]+) desde la posición calculada', phrase, re.IGNORECASE),
                re.search(r'crear campo\s+[\w]+ concatenando ([^f]+)', phrase, re.IGNORECASE)
            ]):
                match_simple = re.search(r'crear campo\s+([\w, ]+)', phrase, re.IGNORECASE)
                if match_simple:
                    field_list = [f.strip() for f in match_simple.group(1).split(',') if f.strip()]
                    for f in field_list:
                        fields.append((f, False, phrase, None, None, None, False))
        return fields

    def _extract_arrayelem_operation_for_field(self, text: str, field: str, source_field: str = None, position: str = None, split_char: str = None, default_val: str = None) -> Optional[Dict]:
        if source_field and position and split_char:
            position_idx = 0 if position.lower() == "primer" else 1
            # Traducir 'punto' a '.' en cualquier contexto
            if split_char.strip().lower() == "punto":
                split_char = "."
            # Buscar si hay default_val en el texto original
            if not default_val:
                default_match = re.search(r'o "([^"]*)" si es nulo', text, re.IGNORECASE)
                if default_match:
                    default_val = default_match.group(1)
            if default_val:
                return {
                    "$ifNull": [
                        {"$arrayElemAt": [
                            {"$split": [{"$toString": {"$toDecimal": f"${source_field}"}}, split_char]},
                            position_idx
                        ]},
                        default_val
                    ]
                }
            else:
                return {
                    "$arrayElemAt": [
                        {"$split": [{"$toString": {"$toDecimal": f"${source_field}"}}, split_char]},
                        position_idx
                    ]
                }
        return None

    def _extract_substr_complex_operation_for_field(self, text: str, field: str, concat_parts: str = None) -> Optional[Dict]:
        if concat_parts:
            # Parsear las partes de concatenación
            parts = [p.strip().strip('"') for p in re.findall(r'"[^"]*"|\w+', concat_parts)]
            concat_list = []
            for part in parts:
                if part.startswith('"') and part.endswith('"'):
                    concat_list.append(part.strip('"'))
                else:
                    concat_list.append(f"${part}")
            
            return {
                "$substr": [
                    {"$concat": concat_list},
                    {"$subtract": [
                        {"$strLenCP": {"$concat": concat_list}},
                        15
                    ]},
                    {"$strLenCP": {"$concat": concat_list}}
                ]
            }
        return None

    def _extract_concat_complex_operation_for_field(self, text: str, field: str, concat_parts: str = None, context_fields: dict = None) -> Optional[Dict]:
        if concat_parts:
            # Parsear las partes de concatenación
            parts = [p.strip().strip('"') for p in re.findall(r'"[^"]*"|\w+', concat_parts)]
            concat_list = []
            for part in parts:
                if part.startswith('"') and part.endswith('"'):
                    concat_list.append(part.strip('"'))
                elif part in context_fields:
                    concat_list.append(f"${part}")
                elif part.startswith('_id.'):
                    concat_list.append(part)
                else:
                    concat_list.append(f"${part}")
            return {"$concat": concat_list}
        return None

    def _validate_pipeline(self):
        """
        🧠 VALIDACIÓN DE PIPELINE - PRINCIPIO DE "SMBOP"
        
        Valida que el pipeline generado sea correcto y completo.
        Implementa validación semántica de la estructura del pipeline.
        """
        if not self.pipeline:
            return
        
        # Validar que cada stage tenga la estructura correcta
        for i, stage in enumerate(self.pipeline):
            if not isinstance(stage, dict):
                print(f"⚠️  Stage {i} no es un diccionario válido")
                continue
            
            # Validar operadores conocidos
            operators = list(stage.keys())
            valid_operators = ['$unwind', '$group', '$project', '$sort', '$match', '$limit', '$skip']
            
            for op in operators:
                if op not in valid_operators:
                    print(f"⚠️  Operador desconocido en stage {i}: {op}")

    def _normalize_id_reference(self, value):
        import re
        if isinstance(value, str) and value.startswith('$_id.'):
            # Reemplaza cualquier cantidad de espacios y $ después de $_id. por un solo punto
            return re.sub(r'\$_id\.[\s\$]+', '$_id.', value)
        return value

def main():
    """
    🚀 FUNCIÓN PRINCIPAL - DEMOSTRACIÓN DE PRINCIPIOS
    
    Muestra cómo se aplican los principios de:
    - "Bridging the Gap": Normalización y mapeo
    - "SmBoP": Parsing secuencial y construcción bottom-up
    """
    
    # Crear generador con dataset por defecto
    generator = SmartMongoQueryGenerator()
    
    import sys
    # Verificar si hay datos en stdin (pipe)
    if not sys.stdin.isatty():
        # Leer desde stdin
        lines = sys.stdin.readlines()
        collection = lines[0].strip()
        query_lines = []
        for line in lines[1:]:
            line = line.strip()
            if line.lower() == 'fin':
                break
            query_lines.append(line)
    else:
        # Modo interactivo
        collection = input("\nNombre de la colección: ")
        print("Ingrese su consulta (escriba 'fin' en nueva línea para terminar):")
        
        query_lines = []
        while True:
            line = input()
            if line.lower() == 'fin':
                break
            query_lines.append(line)
    
    natural_query = "\n".join(query_lines)
    print("\n=== Consulta Generada ===")
    print("[DEBUG] Texto recibido en parse_natural_language:", repr(natural_query))
    pipeline = generator.parse_natural_language(natural_query)
    if not pipeline:
        normalized_text = generator._normalize_text(natural_query)
        generator._process_query_components(normalized_text)
        generator._validate_pipeline()
        pipeline = generator.pipeline
    
    # Generar query final en formato Mongo Shell
    # generated_query = f'db.getCollection("{collection}").aggregate({to_mongo_shell_syntax(pipeline, indent=2, level=1)})'
    

    # Generar query final como string JSON del pipeline
    generated_query = json.dumps(pipeline, indent=2, ensure_ascii=False)

    # 🧠 Aprender del patrón generado (SmBoP)
    if generator.dataset_manager:
        generator.dataset_manager.learn_from_query(collection, natural_query, generated_query)

    return generated_query

if __name__ == "__main__":
    main()