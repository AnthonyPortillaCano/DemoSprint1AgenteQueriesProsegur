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
from dataset_manager import DatasetManager, create_default_dataset

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
        lines = [l.strip() for l in natural_text.split('\n') if l.strip()]
        
        # 🔄 PASO 1: Procesar $unwind primero (SmBoP - Orden Secuencial)
        for line in lines:
            if any(x in line.lower() for x in ["desanidar", "unwind", "expandir"]):
                if "devices hasta transactions" in line.lower() or "devices hasta transactions" in line:
                    pipeline.extend([
                        {"$unwind": "$Devices"},
                        {"$unwind": "$Devices.ServicePoints"},
                        {"$unwind": "$Devices.ServicePoints.ShipOutCycles"},
                        {"$unwind": "$Devices.ServicePoints.ShipOutCycles.Transactions"}
                    ])
                break
        
        # 📊 PASO 2: Procesar $group (SmBoP - Construcción Bottom-up)
        for line in lines:
            if any(x in line.lower() for x in ["agrupar por", "group by"]):
                group_stage = self._build_group_stage_from_text(line)
                if group_stage:
                    pipeline.append(group_stage)
                break
        
        # 🎯 PASO 3: Procesar $project (SmBoP - Parsers Especializados)
        for line in lines:
            if any(x in line.lower() for x in ["crear campo", "concatenando", "que sea", "que convierta", "con padding"]):
                # Procesa cada línea como instrucción completa
                self.pipeline = []
                self._process_simple_query(line)
                if self.pipeline:
                    # Solo agrega si la instrucción generó algo
                    for stage in self.pipeline:
                        # Fusiona los $project si ya existe uno (SmBoP - Acumulación)
                        if "$project" in stage and any("$project" in s for s in pipeline):
                            for s in pipeline:
                                if "$project" in s:
                                    s["$project"].update(stage["$project"])
                                    break
                        else:
                            pipeline.append(stage)
                    self.pipeline = []
        
        # 📈 PASO 4: Procesar $sort (SmBoP - Orden Secuencial)
        for line in lines:
            if any(x in line.lower() for x in ["ordenar por", "sort by"]):
                sort_stage = self._build_sort_stage_from_text(line)
                if sort_stage:
                    pipeline.append(sort_stage)
                break
        
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
        
        # Generar pipeline
        self.pipeline = self.parse_natural_language(natural_text)
        if not self.pipeline:
            normalized_text = self._normalize_text(natural_text)
            self._process_query_components(normalized_text)
            self._validate_pipeline()
        
        # Generar query final
        generated_query = f'db.getCollection("{collection}").aggregate({json.dumps(self.pipeline, indent=2)})'
        
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
            self._process_simple_query(text)

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
        match = re.search(r'(?:agrupar por|group by) ([\w\., y()%]+)', text)
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

        # Extraer campos de proyección desde la instrucción
        project_fields = []
        match = re.search(r'(?:proyectar|project) ([\w,\. ]+)', text_lower)
        if match:
            project_fields = [self._normalize_field(f.strip()) for f in match.group(1).split(",") if f.strip()]
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
                # Plantilla de reg (puedes hacerla más dinámica si lo deseas)
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
                stage1["totalParteEntera"] = {"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$total"}}, "."]}, 0]}
            elif field == "totalpartedecimal":
                stage1["totalParteDecimal"] = {"$ifNull": [{"$concat": [{"$arrayElemAt": [{"$split": [{"$toString": {"$toDecimal": "$total"}}, "."]}, 1]}, "0"]}, "00"]}
            elif "." in field:
                # Permitir rutas anidadas
                stage1[field.split(".")[-1]] = f"${field}"
            else:
                # Proyección directa
                stage1[field] = f"$_id.{field}" if field in ["deviceId", "branchCode", "currencyCode", "subChannelCode", "shipOutCode", "confirmationCode"] else 1

        stages.append({"$project": stage1})
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
        match = re.search(r'(?:agrupar por|group by) ([\w\., y()%]+)', text, re.IGNORECASE)
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
                # Permitir rutas anidadas (Devices.Id, etc.)
                if "." in field:
                    _id[field.split(".")[-1]] = f"${field}"
                else:
                    _id[field] = f"${field}"

        # Suma si se menciona
        if self._find_operation(text, 'sum'):
            sum_field = "$total"
            # Detectar campo de suma si se menciona explícitamente
            sum_match = re.search(r'suma de ([\w\.]+)', text, re.IGNORECASE)
            if sum_match:
                sum_field = f"${sum_match.group(1)}"
            group_stage = {"$group": {"_id": _id, "total": {"$sum": sum_field}}}
        else:
            group_stage = {"$group": {"_id": _id}}

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

    def _extract_advanced_concat_for_field(self, text: str, field: str, context_fields: dict) -> Optional[Dict]:
        # Ejemplo: crear campo reg concatenando "5", monedaCond, date, "00", deviceIdPad, ...
        import re
        pattern = r'crear campo (\w+) concatenando ([^\n]+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match and match.group(1).lower() == field.lower():
            _, concat_args = match.groups()
            # Separa por coma, respeta strings entre comillas
            parts = [p.strip() for p in re.findall(r'"[^"]*"|\w+', concat_args)]
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

    def _extract_advanced_padding_operation_for_field(self, text: str, field: str) -> Optional[Dict]:
        # Ejemplo: crear campo deviceIdPad con padding izquierda 20 de deviceId usando $sum y $strLenCP
        import re
        pattern = r'crear campo (\w+) con padding izquierda (\d+) de (\w+) usando \$sum y \$strLenCP'
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
                    {"$sum": [
                        {"$strLenCP": {"$concat": [pad_str, source_field]}},
                        -pad_len
                    ]},
                    pad_len
                ]
            }
        return None

    def _process_simple_query(self, text: str):
        projection = {"_id": 0}
        context_fields = {}
        fields_to_create = self._extract_fields_to_create(text)
        for field, is_special, phrase, date_field, fmt, substr_len, is_concat in fields_to_create:
            # Debug temporal
            if "substring" in phrase.lower() or "split" in phrase.lower() or "concatenando" in phrase.lower():
                print(f"DEBUG: field={field}, is_special={is_special}, date_field={date_field}, fmt={fmt}, substr_len={substr_len}")
            if is_special:
                # totalParteEntera
                if field == 'totalParteEntera':
                    projection[field] = {
                        "$arrayElemAt": [
                            {"$split": [
                                {"$toString": {"$toDecimal": "$total"}},
                                "."
                            ]},
                            0
                        ]
                    }
                    context_fields[field] = projection[field]
                    continue
                # totalParteDecimal
                if field == 'totalParteDecimal':
                    default_val = substr_len if substr_len else "00"
                    projection[field] = {
                        "$ifNull": [
                            {"$concat": [
                                {"$arrayElemAt": [
                                    {"$split": [
                                        {"$toString": {"$toDecimal": "$total"}},
                                        "."
                                    ]},
                                    1
                                ]},
                                "0"
                            ]},
                            default_val
                        ]
                    }
                    context_fields[field] = projection[field]
                    continue
                # amountPad
                if field == 'amountPad':
                    projection[field] = {
                        "$substr": [
                            {"$concat": [
                                "0000000000000",
                                "$totalParteEntera",
                                {"$substr": ["$totalParteDecimal", 0, 2]}
                            ]},
                            {"$subtract": [
                                {"$strLenCP": {"$concat": ["0000000000000", "$totalParteEntera", "00"]}},
                                15
                            ]},
                            15
                        ]
                    }
                    context_fields[field] = projection[field]
                    continue
                if is_concat:
                    concat_op = self._extract_concat_operation_for_field(phrase, field)
                    if concat_op:
                        projection[field] = concat_op
                        context_fields[field] = concat_op
                        continue
                elif date_field and fmt and fmt not in ["primer", "segundo"]:
                    date_conv = self._create_date_conversion(date_field, fmt, substr_len)
                    projection[field] = date_conv
                    context_fields[field] = date_conv
                    continue
                # Substring especial dinámico
                elif date_field and substr_len and fmt is None:
                    print(f"DEBUG: Procesando substring para {field} con source={date_field}, start={substr_len}")
                    substr_op = self._extract_substrcp_operation_for_field(phrase, field, date_field, substr_len)
                    if substr_op:
                        projection[field] = substr_op
                        context_fields[field] = substr_op
                        continue
                # Operaciones con $arrayElemAt y $split
                elif date_field and fmt and fmt in ["primer", "segundo"]:
                    arrayelem_op = self._extract_arrayelem_operation_for_field(phrase, field, date_field, fmt, substr_len)
                    if arrayelem_op:
                        projection[field] = arrayelem_op
                        context_fields[field] = arrayelem_op
                        continue
                # Operaciones con $substr complejas
                elif date_field and not fmt and not substr_len and "concatenación" in phrase:
                    substr_complex_op = self._extract_substr_complex_operation_for_field(phrase, field, date_field)
                    if substr_complex_op:
                        projection[field] = substr_complex_op
                        context_fields[field] = substr_complex_op
                        continue
                # Operaciones de concatenación complejas
                elif date_field and not fmt and not substr_len and "concatenando" in phrase:
                    concat_complex_op = self._extract_concat_complex_operation_for_field(phrase, field, date_field, context_fields)
                    if concat_complex_op:
                        projection[field] = concat_complex_op
                        context_fields[field] = concat_complex_op
                        continue
                # Nuevos: $substrCP, $ifNull, $cond, padding, concat avanzado, padding avanzado
                adv_padding_op = self._extract_advanced_padding_operation_for_field(phrase, field)
                if adv_padding_op:
                    projection[field] = adv_padding_op
                    context_fields[field] = adv_padding_op
                    continue
                substrcp_op = self._extract_substrcp_operation_for_field(phrase, field)
                if substrcp_op:
                    projection[field] = substrcp_op
                    context_fields[field] = substrcp_op
                    continue
                ifnull_op = self._extract_ifnull_operation_for_field(phrase, field)
                if ifnull_op:
                    projection[field] = ifnull_op
                    context_fields[field] = ifnull_op
                    continue
                cond_op = self._extract_cond_operation_for_field(phrase, field)
                if cond_op:
                    projection[field] = cond_op
                    context_fields[field] = cond_op
                    continue
                padding_op = self._extract_padding_operation_for_field(phrase, field)
                if padding_op:
                    projection[field] = padding_op
                    context_fields[field] = padding_op
                    continue
                adv_concat_op = self._extract_advanced_concat_for_field(phrase, field, context_fields)
                if adv_concat_op:
                    projection[field] = adv_concat_op
                    context_fields[field] = adv_concat_op
                    continue
                # Si no se reconoce como especial, no lo agregues
                continue
            else:
                projection[field] = f"${field}"
                context_fields[field] = f"${field}"
        if projection:
            self.pipeline.append({"$project": projection})

    def _is_special_field(self, text: str, field: str) -> bool:
        text_lower = text.lower()
        field_lower = field.lower()
        return (
            f'con formato' in text_lower and field_lower in text_lower or
            f'fecha {field_lower}' in text_lower or
            f'convertir {field_lower}' in text_lower or
            f'formato concat' in text_lower and field_lower in text_lower
        )

    def _extract_concat_operation_for_field(self, text: str, field: str) -> Optional[Dict]:
        # Mejorado: soporta concat para reg con fecha formateada
        pattern = (
            fr'crear campo {field}\s+con formato concat\(([^)]*)\)'
        )
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Extrae los argumentos del concat
            args = [a.strip().strip('"') for a in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', match.group(1))]
            concat_parts = []
            for arg in args:
                # Si es una instrucción de fecha, la procesa
                fecha_match = re.match(r'fecha (\w+) como (\w+)(?: usando los primeros (\d+) caracteres)?', arg, re.IGNORECASE)
                if fecha_match:
                    date_field, date_fmt, substr_len = fecha_match.groups()
                    # Usar el campo correcto según el contexto
                    if date_field.lower() == "date":
                        # Si se especifica substr_len, crear la conversión completa
                        if substr_len:
                            concat_parts.append(self._create_date_conversion("date", date_fmt, substr_len))
                        else:
                            concat_parts.append("$_id.date")
                    else:
                        concat_parts.append(self._create_date_conversion(date_field, date_fmt, substr_len))
                else:
                    concat_parts.append(arg)
            return {"$concat": concat_parts}
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
    
    natural_query = " ".join(query_lines)
    print("\n=== Consulta Generada ===")
    print(generator.generate_query(collection, natural_query))

if __name__ == "__main__":
    main()