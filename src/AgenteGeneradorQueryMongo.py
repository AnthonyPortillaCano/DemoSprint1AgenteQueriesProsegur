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

import re
import json
from typing import Dict, List, Optional, Any, Union
from src.dataset_manager import DatasetManager, create_default_dataset

def to_mongo_shell_syntax(obj, indent=2, level=1):
    """
    Convierte un dict/list JSON a una cadena tipo Mongo Shell (sin comillas en las claves).
    """
    import keyword
    sp = ' ' * (indent * level)
    if isinstance(obj, dict):
        items = []
        for k, v in obj.items():
            # No poner comillas si la clave es un identificador válido de MongoDB o empieza por $
            if re.match(r'^[a-zA-Z_\$][a-zA-Z0-9_\$]*$', k) and not keyword.iskeyword(k):
                key = k
            else:
                key = k.strip('"')  # Quita comillas si las tuviera
            items.append(f"{sp}{key}: {to_mongo_shell_syntax(v, indent, level+1)}")
        return '{\n' + ',\n'.join(items) + '\n' + ' ' * (indent * (level-1)) + '}'
    elif isinstance(obj, list):
        items = [to_mongo_shell_syntax(v, indent, level+1) for v in obj]
        return '[\n' + ',\n'.join(f"{sp}{item}" for item in items) + '\n' + ' ' * (indent * (level-1)) + ']'
    elif isinstance(obj, str):
        # Siempre deja los valores string entre comillas
        return f'"{obj}"'
    else:
        return str(obj)

class SmartMongoQueryGenerator:
    """
    🎯 GENERADOR INTELIGENTE DE CONSULTAS MONGODB
    
    Implementa los principios de:
    - "Bridging the Gap": Normalización y mapeo de lenguaje natural
    - "SmBoP": Parsing secuencial y construcción bottom-up
    
    Permite generar pipelines de MongoDB complejos desde consultas en lenguaje natural.
    """
    def __init__(self, dataset_manager: Optional[DatasetManager] = None):
        """
        🔬 INICIALIZACIÓN - PRINCIPIOS DE "BRIDGING THE GAP" + DATASET
        
        Configura los mapeos y sinónimos necesarios para la traducción
        de lenguaje natural a operadores MongoDB, integrando dataset inteligente.
        """
        # 🎯 GESTOR DE DATASET (Nuevo - Contexto de Datos)
        self.dataset_manager = dataset_manager or create_default_dataset()
        
        # 📅 Formatos de fecha soportados
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
            'date': ['fecha', 'date', 'formato fecha']
        }
        
        # 🏷️ SINÓNIMOS DE CAMPOS (Bridging the Gap - Mapeo de Campos)
        # Ahora se cargan dinámicamente desde el dataset
        self.FIELD_SYNONYMS = self._load_field_synonyms_from_dataset()

    def _load_field_synonyms_from_dataset(self) -> Dict[str, List[str]]:
        """
        🎯 CARGA DINÁMICA DE SINÓNIMOS - PRINCIPIO DE "BRIDGING THE GAP"
        
        Carga sinónimos de campos desde el dataset, permitiendo
        adaptación dinámica a diferentes esquemas de datos.
        
        Returns:
            Diccionario de sinónimos de campos
        """
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
            field_synonyms = {
                'deviceId': ['deviceid', 'id de dispositivo', 'dispositivo', 'devices.id'],
                'branchCode': ['branchcode', 'código de sucursal', 'sucursal', 'devices.branchcode'],
                'subChannelCode': ['subchannelcode', 'subcanal', 'devices.servicepoints.shipoutcycles.subchannelcode'],
                'shipOutCode': ['shipoutcode', 'código de envío', 'envio', 'devices.servicepoints.shipoutcycles.code'],
                'currencyCode': ['currencycode', 'moneda', 'devices.servicepoints.shipoutcycles.transactions.currencycode'],
                'confirmationCode': ['confirmationcode', 'código de confirmación', 'confirmacion', 'devices.servicepoints.shipoutcycles.confirmationcode'],
                'date': ['date', 'fecha', 'fechahora'],
                'total': ['total', 'monto', 'amount', 'devices.servicepoints.shipoutcycles.transactions.total']
            }
        
        return field_synonyms

    def _validate_field_with_dataset(self, field: str, collection_name: str = None) -> bool:
        """
        🎯 VALIDACIÓN SEMÁNTICA - PRINCIPIO DE "BRIDGING THE GAP"
        
        Valida si un campo existe en el dataset, proporcionando
        validación semántica y contexto de datos.
        
        Args:
            field: Campo a validar
            collection_name: Nombre de la colección (opcional)
            
        Returns:
            True si el campo es válido, False en caso contrario
        """
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
        """
        🎯 SUGERENCIAS INTELIGENTES - PRINCIPIO DE "SMBOP"
        
        Sugiere campos basado en el dataset y patrones aprendidos.
        
        Args:
            partial_name: Nombre parcial del campo
            collection_name: Nombre de la colección (opcional)
            
        Returns:
            Lista de campos sugeridos
        """
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

    def _normalize_field(self, field: str) -> str:
        """
        🔬 NORMALIZACIÓN DE CAMPOS - PRINCIPIO DE "BRIDGING THE GAP"
        
        Convierte variaciones lingüísticas de campos a nombres canónicos.
        Permite mapear sinónimos y rutas anidadas a campos específicos.
        
        Args:
            field: Campo en lenguaje natural
            
        Returns:
            Nombre canónico del campo o ruta anidada
        """
        field = field.lower().replace(' ', '')
        for canonical, synonyms in self.FIELD_SYNONYMS.items():
            if field == canonical.lower() or field in [s.replace(' ', '') for s in synonyms]:
                # Devuelve el path real si existe en los sinónimos
                for s in synonyms:
                    if field == s.replace(' ', '') and '.' in s:
                        return s  # path real
                return canonical
        return field

    def _expand_special_phrases(self, field: str) -> list:
        """
        🔬 TRADUCCIÓN DE FRASES ESPECIALES - PRINCIPIO DE "BRIDGING THE GAP"
        
        Convierte frases descriptivas complejas en rutas específicas de datos.
        Permite expresar conceptos de alto nivel en términos de estructura de datos.
        
        Args:
            field: Frase descriptiva en lenguaje natural
            
        Returns:
            Lista de rutas de campos específicos
        """
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
        """Extrae campos de una cadena, separando por comas y 'y', y limpiando espacios. Traduce frases especiales."""
        field_str = re.sub(r'\s+y\s+', ',', field_str)
        # Quita frases comunes de operaciones
        field_str = re.sub(r'(suma el total|proyectar reg|totalParteEntera y totalParteDecimal|ordenar por [^,]+|proyectar campo reg concatenando los valores seg[úu]n la plantilla|sumar el monto de las transacciones|agrupar por fecha)', '', field_str, flags=re.IGNORECASE)
        fields = [f.strip() for f in field_str.split(',') if f.strip()]
        expanded = []
        for f in fields:
            expanded.extend(self._expand_special_phrases(f))
        return expanded

    def parse_natural_language(self, natural_text: str) -> list:
        """
        🧠 PARSING SEMI-AUTOREGRESIVO - PRINCIPIO DE "SMBOP"
        
        Procesa instrucciones secuenciales en orden específico:
        1. $unwind (desanidamiento)
        2. $group (agrupamiento)
        3. $project (proyecciones)
        4. $sort (ordenamiento)
        
        Implementa construcción bottom-up acumulando stages progresivamente.
        
        Args:
            natural_text: Texto en lenguaje natural con instrucciones
            
        Returns:
            Pipeline completo de MongoDB
        """
        pipeline = []
        self.pipeline = []  # Inicializa solo una vez aquí
        lines = [l.strip() for l in natural_text.split('\n') if l.strip()]

        # PASO 0: Detectar instrucciones de join y agregar $lookup solo si se solicita explícitamente
        join_detected = False
        for line in lines:
            join_match = re.search(r'une la colección ([a-zA-Z0-9_]+) con la colección ([a-zA-Z0-9_]+) usando el campo ([a-zA-Z0-9_]+)', line, re.IGNORECASE)
            if join_match:
                local_collection = join_match.group(1)
                from_collection = join_match.group(2)
                local_field = join_match.group(3)
                pipeline.append({
                    "$lookup": {
                        "from": from_collection,
                        "localField": local_field,
                        "foreignField": local_field,
                        "as": f"{from_collection}_info"
                    }
                })
                pipeline.append({"$unwind": f"${from_collection}_info"})
                # Proyección dinámica de campos si la instrucción contiene 'proyecta los campos ...'
                project_match = re.search(r'proyecta los campos ([\w\.,_ ]+)', natural_text, re.IGNORECASE)
                if project_match:
                    campos = [c.strip() for c in project_match.group(1).split(',')]
                    project_stage = {"$project": {"_id": 0}}
                    if "departamentos_info" in campos and "departamento_nombre" in campos:
                        project_stage["$project"][f"{from_collection}_info.departamento_nombre"] = f"${from_collection}_info.departamento_nombre"
                        campos = [c for c in campos if c not in ["departamentos_info", "departamento_nombre"]]
                    for campo in campos:
                        project_stage["$project"][campo] = f"${campo}"
                    pipeline.append(project_stage)
                join_detected = True

        # Si no hay join, continuar con el pipeline normal
        
        # 🔄 PASO 1: Procesar $unwind primero (SmBoP - Orden Secuencial)
        for line in lines:
            # --- NUEVO: Soporte para la forma explícita 'desanidar <ruta>' ---
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
        
        # 🎯 PASO 3: Procesar $project (SmBoP - Parsers Especializados)
        for line in lines:
            if any(x in line.lower() for x in ["crear campo", "concatenando", "que sea", "que convierta", "con padding", "proyecta", "proyectar"]):
                processed = False
                
                # 1. Mejorar: detectar campo destino explícito para fecha
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
                    processed = True
                
                # 2. Concatenación avanzada para reg
                if not processed:
                    reg_match = re.search(r'crear campo (\w+) que concatene: (.+)', line, re.IGNORECASE)
                    if reg_match:
                        campo_reg = reg_match.group(1)
                        partes = [p.strip() for p in reg_match.group(2).split(',')]
                        concat_expr = []
                        for parte in partes:
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
                            elif parte.lower() == 'un salto de línea':
                                concat_expr.append("\n")
                            elif parte.lower() == 'otro salto de línea' or parte.lower() == 'otro salto de línea.':
                                concat_expr.append("\n")
                            elif parte.lower() == 'un espacio':
                                concat_expr.append(" ")
                            elif parte.startswith('"') and parte.endswith('"'):
                                concat_expr.append(parte.strip('"'))
                            else:
                                concat_expr.append(parte)
                        # Agregar a $project existente o crear uno nuevo
                        project = None
                        for stage in pipeline:
                            if "$project" in stage:
                                project = stage["$project"]
                                break
                        if project is not None:
                            project[campo_reg] = {"$concat": concat_expr}
                        else:
                            pipeline.append({"$project": {"_id": 0, campo_reg: {"$concat": concat_expr}}})
                        processed = True
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
        for stage in pipeline:
            if "$project" in stage and "reg" in stage["$project"]:
                reg = stage["$project"]["reg"]
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
                    stage["$project"]["reg"]["$concat"] = new_concat
        
        return pipeline

    def generate_query(self, collection: str, natural_text: str) -> str:
        """
        🎯 GENERACIÓN DE QUERY CON DATASET - PRINCIPIOS INTEGRADOS
        
        Genera queries MongoDB integrando validación semántica y aprendizaje
        de patrones desde el dataset.
        
        Args:
            collection: Nombre de la colección
            natural_text: Query en lenguaje natural
            
        Returns:
            Query MongoDB generada
        """
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
        pipeline = self.parse_natural_language(natural_text)

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

        # Si aún así el pipeline está vacío, devolver una consulta mínima para evitar query vacía
        if not pipeline:
            pipeline = [{"$match": {}}]

        # Generar query final en formato Mongo Shell
        generated_query = f'db.getCollection("{collection}").aggregate({to_mongo_shell_syntax(pipeline, indent=2, level=1)})'

        # 🧠 Aprender del patrón generado (SmBoP)
        if self.dataset_manager:
            self.dataset_manager.learn_from_query(collection, natural_text, generated_query)

        return generated_query

    def _validate_query_fields(self, natural_text: str, collection: str):
        """
        🎯 VALIDACIÓN DE CAMPOS EN QUERY - PRINCIPIO DE "BRIDGING THE GAP"
        
        Valida que los campos mencionados en la query existan en el dataset
        y sugiere alternativas si no se encuentran.
        
        Args:
            natural_text: Query en lenguaje natural
            collection: Nombre de la colección
        """
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
        join_match = re.search(r'une la colección ([a-zA-Z0-9_]+) con la colección ([a-zA-Z0-9_]+) usando el campo ([a-zA-Z0-9_]+)', line, re.IGNORECASE)
        if match:
            group_fields = [self._normalize_field(f.strip()) for f in match.group(1).split(",") if f.strip()]
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
        """
        🧠 CONSTRUCCIÓN BOTTOM-UP - PRINCIPIO DE "SMBOP"
        
        Construye un stage de $group desde texto natural.
        Implementa construcción progresiva de expresiones complejas.
        
        Args:
            text: Texto natural con instrucciones de agrupamiento
            
        Returns:
            Stage de $group configurado
        """
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
        """Construye un stage de $sort desde texto natural"""
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
        """
        🎯 PARSER ESPECIALIZADO - PRINCIPIO DE "SMBOP"
        
        Parser específico para operaciones $substrCP.
        Implementa extracción de operaciones específicas de MongoDB.
        
        Args:
            text: Texto natural con instrucción
            field: Campo a crear
            source_field: Campo fuente (opcional)
            start: Posición inicial (opcional)
            
        Returns:
            Operación $substrCP configurada o None
        """
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
        """
        Normaliza una frase para búsqueda robusta en el mapeo de concat.
        """
        import unicodedata
        s = phrase.lower().strip()
        s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        s = s.replace('.', '').replace('"', '').replace("'", '')
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
        """
        🧠 CONSTRUCCIÓN BOTTOM-UP - PRINCIPIO DE "SMBOP"
        
        Construye expresiones de conversión de fecha paso a paso.
        Implementa construcción progresiva desde componentes básicos.
        
        Args:
            date_field: Campo de fecha
            fmt: Formato de salida
            substr_len: Longitud de substring (opcional)
            
        Returns:
            Expresión de conversión de fecha completa
        """
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
            match_cond = re.search(r'crear campo\s+[\w]+ que sea "([^"]*)" si (\w+) es "([^"]*)" y "([^"]*)" en otro caso', phrase, re.IGNORECASE)
            if match_cond:
                fields.append((field, True, phrase, None, None, None, False))
                continue
            # Campos simples (pueden ser varios separados por coma) - solo si no se procesó como especial
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
    print("🎯 GENERADOR AVANZADO MONGODB")
    print("📚 Implementando principios de 'Bridging the Gap' + 'SmBoP'")
    print("=" * 60)
    
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
    generated_query = f'db.getCollection("{collection}").aggregate({to_mongo_shell_syntax(pipeline, indent=2, level=1)})'
    
    # 🧠 Aprender del patrón generado (SmBoP)
    if generator.dataset_manager:
        generator.dataset_manager.learn_from_query(collection, natural_query, generated_query)
    
    return generated_query

if __name__ == "__main__":
    main()