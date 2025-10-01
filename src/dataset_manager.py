"""
🎯 GESTOR DE DATASET PARA GENERACIÓN DINÁMICA DE QUERIES

Este módulo implementa un sistema de gestión de dataset que permite:
- Definir esquemas de colecciones MongoDB
- Validar campos y rutas anidadas
- Generar queries contextualizadas
- Aprender patrones de uso
- Sugerir campos y operaciones

📚 PAPERS IMPLEMENTADOS:
- "Bridging the Gap": Validación semántica y contexto de datos
- "SmBoP": Aprendizaje de patrones y adaptación dinámica
"""

import json
import os
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class FieldDefinition:
    """Definición de un campo en el dataset"""
    name: str
    type: str  # string, number, date, boolean, array, object
    path: str  # ruta completa en la estructura
    description: str
    examples: List[str]
    synonyms: List[str]
    is_required: bool = False
    is_indexed: bool = False
    validation_rules: Optional[Dict] = None

@dataclass
class CollectionSchema:
    """Esquema de una colección MongoDB"""
    name: str
    description: str
    fields: Dict[str, FieldDefinition]
    indexes: List[Dict]
    sample_documents: List[Dict]
    query_patterns: List[str]
    created_at: datetime
    updated_at: datetime

class DatasetManager:
    """
    🎯 GESTOR DE DATASET INTELIGENTE
    
    Implementa principios de:
    - "Bridging the Gap": Validación semántica y contexto
    - "SmBoP": Aprendizaje adaptativo de patrones
    """
    
    def __init__(self, dataset_path: str = None):
        """
        Inicializa el gestor de dataset
        
        Args:
            dataset_path: Ruta donde se almacenan los datasets
        """
        # Siempre usar la ruta relativa a la raíz del proyecto
        if dataset_path is None:
            dataset_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datasets/'))
        self.dataset_path = dataset_path
        self.schemas: Dict[str, CollectionSchema] = {}
        self.query_history: List[Dict] = []
        self.learned_patterns: Dict[str, List[str]] = {}
        
        # Crear directorio si no existe
        os.makedirs(dataset_path, exist_ok=True)
        
        # Cargar datasets existentes
        self._load_existing_datasets()
    
    def _load_existing_datasets(self):
        """Carga datasets existentes desde archivos JSON"""
        for filename in os.listdir(self.dataset_path):
            if filename.endswith('.json'):
                collection_name = filename.replace('.json', '')
                self.load_schema(collection_name)
    
    def create_schema(self, collection_name: str, description: str = "") -> CollectionSchema:
        """
        Crea un nuevo esquema de colección
        
        Args:
            collection_name: Nombre de la colección
            description: Descripción de la colección
            
        Returns:
            Esquema de colección creado
        """
        schema = CollectionSchema(
            name=collection_name,
            description=description,
            fields={},
            indexes=[],
            sample_documents=[],
            query_patterns=[],
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.schemas[collection_name] = schema
        return schema
    
    def add_field(self, collection_name: str, field_def: FieldDefinition):
        """
        Agrega un campo al esquema de una colección
        
        Args:
            collection_name: Nombre de la colección
            field_def: Definición del campo
        """
        if collection_name not in self.schemas:
            self.create_schema(collection_name)
        
        self.schemas[collection_name].fields[field_def.name] = field_def
        self.schemas[collection_name].updated_at = datetime.now()
    
    def add_sample_document(self, collection_name: str, document: Dict):
        """
        Agrega un documento de ejemplo al esquema
        
        Args:
            collection_name: Nombre de la colección
            document: Documento de ejemplo
        """
        if collection_name not in self.schemas:
            self.create_schema(collection_name)
        
        self.schemas[collection_name].sample_documents.append(document)
        self.schemas[collection_name].updated_at = datetime.now()
    
    def add_query_pattern(self, collection_name: str, pattern: str):
        """
        Agrega un patrón de query al esquema
        
        Args:
            collection_name: Nombre de la colección
            pattern: Patrón de query en lenguaje natural
        """
        if collection_name not in self.schemas:
            self.create_schema(collection_name)
        
        self.schemas[collection_name].query_patterns.append(pattern)
        self.schemas[collection_name].updated_at = datetime.now()
    
    def validate_field(self, collection_name: str, field_path: str) -> bool:
        """
        Valida si un campo existe en el esquema
        
        Args:
            collection_name: Nombre de la colección
            field_path: Ruta del campo
            
        Returns:
            True si el campo existe, False en caso contrario
        """
        if collection_name not in self.schemas:
            return False
        
        schema = self.schemas[collection_name]
        
        # Buscar campo exacto
        if field_path in schema.fields:
            return True
        
        # Buscar por sinónimos
        for field_name, field_def in schema.fields.items():
            if field_path in field_def.synonyms:
                return True
        
        return False
    
    def get_field_info(self, collection_name: str, field_path: str) -> Optional[FieldDefinition]:
        """
        Obtiene información de un campo
        
        Args:
            collection_name: Nombre de la colección
            field_path: Ruta del campo
            
        Returns:
            Definición del campo o None si no existe
        """
        if collection_name not in self.schemas:
            return None
        
        schema = self.schemas[collection_name]
        
        # Buscar campo exacto
        if field_path in schema.fields:
            return schema.fields[field_path]
        
        # Buscar por sinónimos
        for field_name, field_def in schema.fields.items():
            if field_path in field_def.synonyms:
                return field_def
        
        return None
    
    def suggest_fields(self, collection_name: str, partial_name: str) -> List[str]:
        """
        Sugiere campos basado en un nombre parcial
        
        Args:
            collection_name: Nombre de la colección
            partial_name: Nombre parcial del campo
            
        Returns:
            Lista de campos sugeridos
        """
        if collection_name not in self.schemas:
            return []
        
        schema = self.schemas[collection_name]
        suggestions = []
        
        for field_name, field_def in schema.fields.items():
            if partial_name.lower() in field_name.lower():
                suggestions.append(field_name)
            elif any(partial_name.lower() in syn.lower() for syn in field_def.synonyms):
                suggestions.append(field_name)
        
        return suggestions
    
    def get_related_fields(self, collection_name: str, field_name: str) -> List[str]:
        """
        Obtiene campos relacionados basado en patrones de uso
        
        Args:
            collection_name: Nombre de la colección
            field_name: Nombre del campo
            
        Returns:
            Lista de campos relacionados
        """
        if collection_name not in self.learned_patterns:
            return []
        
        patterns = self.learned_patterns[collection_name]
        related = []
        
        for pattern in patterns:
            if field_name in pattern:
                # Extraer otros campos del patrón
                fields = pattern.split()
                related.extend([f for f in fields if f != field_name])
        
        return list(set(related))
    
    def learn_from_query(self, collection_name: str, natural_query: str, generated_query: str):
        """
        Aprende de una query generada (principio de SmBoP)
        
        Args:
            collection_name: Nombre de la colección
            natural_query: Query en lenguaje natural
            generated_query: Query MongoDB generada
        """
        # Guardar en historial
        self.query_history.append({
            'collection': collection_name,
            'natural_query': natural_query,
            'generated_query': generated_query,
            'timestamp': datetime.now()
        })
        
        # Extraer patrones
        words = natural_query.lower().split()
        if collection_name not in self.learned_patterns:
            self.learned_patterns[collection_name] = []
        
        self.learned_patterns[collection_name].append(' '.join(words))
        
        # Limitar historial
        if len(self.query_history) > 1000:
            self.query_history = self.query_history[-500:]
    
    def save_schema(self, collection_name: str):
        """
        Guarda el esquema en un archivo JSON
        
        Args:
            collection_name: Nombre de la colección
        """
        if collection_name not in self.schemas:
            return
        
        schema = self.schemas[collection_name]
        filepath = os.path.join(self.dataset_path, f"{collection_name}.json")
        
        # Convertir a diccionario
        schema_dict = asdict(schema)
        schema_dict['created_at'] = schema.created_at.isoformat()
        schema_dict['updated_at'] = schema.updated_at.isoformat()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(schema_dict, f, indent=2, ensure_ascii=False)
    
    def load_schema(self, collection_name: str) -> Optional[CollectionSchema]:
        """
        Carga un esquema desde archivo JSON
        
        Args:
            collection_name: Nombre de la colección
            
        Returns:
            Esquema cargado o None si no existe
        """
        filepath = os.path.join(self.dataset_path, f"{collection_name}.json")
        
        if not os.path.exists(filepath):
            return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            schema_dict = json.load(f)
        
        # Convertir campos
        fields = {}
        for field_name, field_data in schema_dict['fields'].items():
            fields[field_name] = FieldDefinition(**field_data)
        
        # Crear esquema
        schema = CollectionSchema(
            name=schema_dict['name'],
            description=schema_dict['description'],
            fields=fields,
            indexes=schema_dict['indexes'],
            sample_documents=schema_dict['sample_documents'],
            query_patterns=schema_dict['query_patterns'],
            created_at=datetime.fromisoformat(schema_dict['created_at']),
            updated_at=datetime.fromisoformat(schema_dict['updated_at'])
        )
        
        self.schemas[collection_name] = schema
        return schema
    
    def get_schema_summary(self, collection_name: str) -> Dict:
        """
        Obtiene un resumen del esquema
        
        Args:
            collection_name: Nombre de la colección
            
        Returns:
            Resumen del esquema
        """
        if collection_name not in self.schemas:
            return {}
        
        schema = self.schemas[collection_name]
        
        return {
            'name': schema.name,
            'description': schema.description,
            'field_count': len(schema.fields),
            'sample_count': len(schema.sample_documents),
            'pattern_count': len(schema.query_patterns),
            'created_at': schema.created_at.isoformat(),
            'updated_at': schema.updated_at.isoformat(),
            'fields': list(schema.fields.keys())
        }
    
    def export_dataset_info(self) -> Dict:
        """
        Exporta información completa del dataset
        
        Returns:
            Información completa del dataset
        """
        return {
            'collections': [self.get_schema_summary(name) for name in self.schemas.keys()],
            'total_queries': len(self.query_history),
            'learned_patterns': self.learned_patterns,
            'dataset_path': self.dataset_path
        }

# 🎯 DATASET PREDEFINIDO PARA EL PROYECTO ACTUAL
def create_default_dataset() -> DatasetManager:
    # Inicializar lista de campos
    fields = []
    # Campos extra para ejemplo de join
    fields += [
        FieldDefinition(
            name="nombre",
            type="string",
            path="nombre",
            description="Nombre del empleado",
            examples=["Juan", "María"],
            synonyms=["nombre", "name", "primer nombre", "nombres", "first name"],
            is_required=False
        ),
        FieldDefinition(
            name="apellido",
            type="string",
            path="apellido",
            description="Apellido del empleado",
            examples=["Pérez", "García"],
            synonyms=["apellido", "apellidos", "last name", "surname", "segundo nombre"],
            is_required=False
        ),
        FieldDefinition(
            name="departamentos_info",
            type="object",
            path="departamentos_info",
            description="Información del departamento",
            examples=["{nombre: 'TI', ubicacion: 'Lima'}"],
            synonyms=["departamentos_info", "info departamento", "información de departamento", "departamento info", "departamento detalles"],
            is_required=False
        ),
        FieldDefinition(
            name="departamento_nombre",
            type="string",
            path="departamento_nombre",
            description="Nombre del departamento",
            examples=["TI", "RRHH"],
            synonyms=["departamento_nombre", "nombre departamento", "nombre del departamento", "department name", "departamento"],
            is_required=False
        )
    ]
    """
    Crea un dataset predefinido para el proyecto actual
    basado en la estructura de Devices, ServicePoints, ShipOutCycles, Transactions
    """
    dataset_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datasets/'))
    manager = DatasetManager(dataset_path=dataset_path)
    
    # Crear esquema para la colección principal
    schema = manager.create_schema(
        "transactions_collection",
        "Colección de transacciones con estructura anidada de dispositivos y puntos de servicio"
    )
    
    # Campos principales
    fields = [
        FieldDefinition(
            name="Date",
            type="date",
            path="Date",
            description="Fecha y hora de la transacción",
            examples=["2024-01-15T10:30:00Z", "2024-01-15 10:30:00"],
            synonyms=["fecha", "fechahora", "timestamp", "date"],
            is_required=True,
            is_indexed=True
        ),
        FieldDefinition(
            name="Devices",
            type="array",
            path="Devices",
            description="Array de dispositivos",
            examples=["[device1, device2]"],
            synonyms=["dispositivos", "devices", "equipos"],
            is_required=True
        ),
        FieldDefinition(
            name="deviceId",
            type="string",
            path="Devices.Id",
            description="Identificador único del dispositivo",
            examples=["DEV001", "DEV002"],
            synonyms=["id de dispositivo", "deviceid", "dispositivo"],
            is_required=True,
            is_indexed=True
        ),
        FieldDefinition(
            name="branchCode",
            type="string",
            path="Devices.BranchCode",
            description="Código de la sucursal",
            examples=["PE240", "PE241"],
            synonyms=["código de sucursal", "branchcode", "sucursal"],
            is_required=True,
            is_indexed=True
        ),
        FieldDefinition(
            name="ServicePoints",
            type="array",
            path="Devices.ServicePoints",
            description="Array de puntos de servicio",
            examples=["[sp1, sp2]"],
            synonyms=["puntos de servicio", "servicepoints", "puntos"],
            is_required=True
        ),
        FieldDefinition(
            name="ShipOutCycles",
            type="array",
            path="Devices.ServicePoints.ShipOutCycles",
            description="Array de ciclos de envío",
            examples=["[cycle1, cycle2]"],
            synonyms=["ciclos de envío", "shipoutcycles", "ciclos"],
            is_required=True
        ),
        FieldDefinition(
            name="Transactions",
            type="array",
            path="Devices.ServicePoints.ShipOutCycles.Transactions",
            description="Array de transacciones",
            examples=["[tx1, tx2]"],
            synonyms=["transacciones", "transactions", "operaciones"],
            is_required=True
        ),
        FieldDefinition(
            name="total",
            type="number",
            path="Devices.ServicePoints.ShipOutCycles.Transactions.Total",
            description="Monto total de la transacción",
            examples=["100.50", "250.75"],
            synonyms=["total", "monto", "amount", "valor"],
            is_required=True
        ),
        FieldDefinition(
            name="currencyCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode",
            description="Código de moneda",
            examples=["PEN", "USD"],
            synonyms=["moneda", "currencycode", "código de moneda"],
            is_required=True
        ),
        FieldDefinition(
            name="subChannelCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.SubChannelCode",
            description="Código del subcanal",
            examples=["CH001", "CH002"],
            synonyms=["subcanal", "subchannelcode", "canal"],
            is_required=True
        ),
        FieldDefinition(
            name="shipOutCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.Code",
            description="Código de envío",
            examples=["SO001", "SO002"],
            synonyms=["código de envío", "shipoutcode", "envio"],
            is_required=True
        ),
        FieldDefinition(
            name="confirmationCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.ConfirmationCode",
            description="Código de confirmación",
            examples=["CONF001", "CONF002"],
            synonyms=["código de confirmación", "confirmationcode", "confirmacion"],
            is_required=True
        )
    ]
    
    # Agregar campos al esquema
    for field in fields:
        manager.add_field("transactions_collection", field)
    
    # Agregar documento de ejemplo
    sample_doc = {
        "Date": "2024-01-15T10:30:00Z",
        "Devices": [
            {
                "Id": "DEV001",
                "BranchCode": "PE240",
                "ServicePoints": [
                    {
                        "ShipOutCycles": [
                            {
                                "SubChannelCode": "CH001",
                                "Code": "SO001",
                                "ConfirmationCode": "CONF001",
                                "Transactions": [
                                    {
                                        "Total": 100.50,
                                        "CurrencyCode": "PEN"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    manager.add_sample_document("transactions_collection", sample_doc)
    
    # Agregar patrones de query comunes
    common_patterns = [
        "desanidar devices hasta transactions",
        "agrupar por fecha deviceid branchcode",
        "crear campo reg con formato concatenado",
        "ordenar por deviceid",
        "suma de total",
        "filtro por moneda PEN"
    ]
    
    for pattern in common_patterns:
        manager.add_query_pattern("transactions_collection", pattern)
    
    # Guardar esquema
    manager.save_schema("transactions_collection")
    
    return manager

if __name__ == "__main__":
    # Crear dataset por defecto
    manager = create_default_dataset()
    print("✅ Dataset creado exitosamente")
    print(f"📊 Esquemas: {list(manager.schemas.keys())}")
    print(f"📝 Campos en transactions_collection: {len(manager.schemas['transactions_collection'].fields)}") 