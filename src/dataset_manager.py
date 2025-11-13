    
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

import os
import json
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
        """Carga datasets existentes desde archivos JSON que sean esquemas válidos (con clave 'fields')."""
        for filename in os.listdir(self.dataset_path):
            if filename.endswith('.json'):
                collection_name = filename.replace('.json', '')
                try:
                    schema = self.load_schema(collection_name, skip_invalid=True)
                    if schema is None:
                        continue
                except Exception:
                    continue
    
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
        # Buscar por sinónimos y por path
        for field_name, field_def in schema.fields.items():
            if field_path in field_def.synonyms:
                return True
            if hasattr(field_def, 'path') and field_path == field_def.path:
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
    
    def load_schema(self, collection_name: str, skip_invalid: bool = False) -> Optional[CollectionSchema]:
        """
        Carga un esquema desde archivo JSON
        
        Args:
            collection_name: Nombre de la colección
        Returns:
            Esquema cargado o None si no existe o si no es válido (cuando skip_invalid=True)
        """
        filepath = os.path.join(self.dataset_path, f"{collection_name}.json")
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r', encoding='utf-8') as f:
            schema_dict = json.load(f)
        # Validar que sea un esquema (debe tener 'fields')
        if 'fields' not in schema_dict:
            if skip_invalid:
                return None
            else:
                raise KeyError(f"El archivo {filepath} no contiene la clave 'fields' y no es un esquema válido.")
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
    def get_fields(self, collection_name: str = None) -> list:
        """
        Devuelve la lista de campos (FieldDefinition) de una colección.
        Si no se especifica collection_name, usa la primera colección disponible.
        """
        if not self.schemas:
            return []
        if collection_name is None:
            # Usar la primera colección si no se especifica
            collection_name = next(iter(self.schemas.keys()))
        schema = self.schemas.get(collection_name)
        if not schema:
            return []
        return list(schema.fields.values())

# 🎯 DATASET PREDEFINIDO PARA EL PROYECTO ACTUAL
def create_default_dataset() -> DatasetManager:
    # Documentos de ejemplo (para sample_documents)
    sample_docs = [
        {
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
                                        {"Total": 100.5, "CurrencyCode": "PEN"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-16T11:45:00Z",
            "Devices": [
                {
                    "Id": "DEV002",
                    "BranchCode": "PE241",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH002",
                                    "Code": "SO002",
                                    "ConfirmationCode": "CONF002",
                                    "Transactions": [
                                        {"Total": 250.75, "CurrencyCode": "USD"},
                                        {"Total": 300.0, "CurrencyCode": "PEN"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-17T09:20:00Z",
            "Devices": [
                {
                    "Id": "DEV003",
                    "BranchCode": "PE242",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH003",
                                    "Code": "SO003",
                                    "ConfirmationCode": "CONF003",
                                    "Transactions": [
                                        {"Total": 500.0, "CurrencyCode": "USD"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-18T14:10:00Z",
            "Devices": [
                {
                    "Id": "DEV004",
                    "BranchCode": "PE243",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH004",
                                    "Code": "SO004",
                                    "ConfirmationCode": "CONF004",
                                    "Transactions": [
                                        {"Total": 120.0, "CurrencyCode": "PEN"},
                                        {"Total": 80.0, "CurrencyCode": "USD"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # ------ 5 adicionales ------
        {
            "Date": "2024-01-19T08:15:00Z",
            "Devices": [
                {
                    "Id": "DEV005",
                    "BranchCode": "PE244",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH005",
                                    "Code": "SO005",
                                    "ConfirmationCode": "CONF005",
                                    "Transactions": [
                                        {"Total": 50.0, "CurrencyCode": "PEN"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-20T12:00:00Z",
            "Devices": [
                {
                    "Id": "DEV006",
                    "BranchCode": "PE245",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH006",
                                    "Code": "SO006",
                                    "ConfirmationCode": "CONF006",
                                    "Transactions": [
                                        {"Total": 700.0, "CurrencyCode": "USD"},
                                        {"Total": 200.0, "CurrencyCode": "PEN"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-21T15:45:00Z",
            "Devices": [
                {
                    "Id": "DEV007",
                    "BranchCode": "PE246",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH007",
                                    "Code": "SO007",
                                    "ConfirmationCode": "CONF007",
                                    "Transactions": [
                                        {"Total": 999.99, "CurrencyCode": "USD"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-22T09:30:00Z",
            "Devices": [
                {
                    "Id": "DEV008",
                    "BranchCode": "PE247",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH008",
                                    "Code": "SO008",
                                    "ConfirmationCode": "CONF008",
                                    "Transactions": [
                                        {"Total": 430.0, "CurrencyCode": "PEN"},
                                        {"Total": 150.0, "CurrencyCode": "USD"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "Date": "2024-01-23T17:50:00Z",
            "Devices": [
                {
                    "Id": "DEV009",
                    "BranchCode": "PE248",
                    "ServicePoints": [
                        {
                            "ShipOutCycles": [
                                {
                                    "SubChannelCode": "CH009",
                                    "Code": "SO009",
                                    "ConfirmationCode": "CONF009",
                                    "Transactions": [
                                        {"Total": 300.0, "CurrencyCode": "USD"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]


    # Definiciones de campos
    field_definitions = [
        FieldDefinition(
            name="Date",
            type="date",
            path="Date",
            description="Fecha y hora de la transacción",
            examples=["2024-01-15T10:30:00Z"],
            synonyms=["fecha", "fechahora", "timestamp", "date"],
            is_required=True,
            is_indexed=True
        ),
        FieldDefinition(
            name="deviceId",
            type="string",
            path="Devices.Id",
            description="Identificador único del dispositivo",
            examples=["DEV001", "DEV002"],
            synonyms=["id de dispositivo", "deviceid", "id", "devices.id"],
            is_required=True,
            is_indexed=True
        ),
        FieldDefinition(
            name="BranchCode",
            type="string",
            path="Devices.BranchCode",
            description="Código de sucursal del dispositivo",
            examples=["PE240", "PE241"],
            synonyms=["branchcode", "código de sucursal", "devices.branchcode"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="ServicePoints",
            type="array",
            path="Devices.ServicePoints",
            description="Puntos de servicio del dispositivo",
            examples=["[{...}]"] ,
            synonyms=["servicepoints", "puntos de servicio", "devices.servicepoints"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="ShipOutCycles",
            type="array",
            path="Devices.ServicePoints.ShipOutCycles",
            description="Ciclos de envío del punto de servicio",
            examples=["[{...}]"] ,
            synonyms=["shipoutcycles", "ciclos de envío", "devices.servicepoints.shipoutcycles"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="Transactions",
            type="array",
            path="Devices.ServicePoints.ShipOutCycles.Transactions",
            description="Transacciones del ciclo de envío",
            examples=["[{...}]"] ,
            synonyms=["transactions", "transacciones", "devices.servicepoints.shipoutcycles.transactions"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="Total",
            type="number",
            path="Devices.ServicePoints.ShipOutCycles.Transactions.Total",
            description="Monto total de la transacción",
            examples=["100.5", "250.75"],
            synonyms=["total", "monto", "amount", "devices.servicepoints.shipoutcycles.transactions.total"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="CurrencyCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.Transactions.CurrencyCode",
            description="Código de moneda de la transacción",
            examples=["PEN", "USD"],
            synonyms=["currencycode", "moneda", "devices.servicepoints.shipoutcycles.transactions.currencycode"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="SubChannelCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.SubChannelCode",
            description="Código de subcanal del ciclo de envío",
            examples=["CH001", "CH002"],
            synonyms=["subchannelcode", "subcanal", "devices.servicepoints.shipoutcycles.subchannelcode"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="Code",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.Code",
            description="Código del ciclo de envío",
            examples=["SO001", "SO002"],
            synonyms=["code", "código", "devices.servicepoints.shipoutcycles.code"],
            is_required=False,
            is_indexed=False
        ),
        FieldDefinition(
            name="ConfirmationCode",
            type="string",
            path="Devices.ServicePoints.ShipOutCycles.ConfirmationCode",
            description="Código de confirmación del ciclo de envío",
            examples=["CONF001", "CONF002"],
            synonyms=["confirmationcode", "código de confirmación", "devices.servicepoints.shipoutcycles.confirmationcode"],
            is_required=False,
            is_indexed=False
        ),
    ]

    dataset_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datasets/'))
    manager = DatasetManager(dataset_path=dataset_path)

    schema = manager.create_schema(
        "transactions_collection",
        "Colección de transacciones con estructura anidada"
    )

    # Agregar definiciones
    for field in field_definitions:
        manager.add_field("transactions_collection", field)

    # Agregar documentos de ejemplo
    for doc in sample_docs:
        manager.add_sample_document("transactions_collection", doc)

    manager.save_schema("transactions_collection")
    return manager


if __name__ == "__main__":
    # Crear dataset por defecto
    manager = create_default_dataset()
    print("✅ Dataset creado exitosamente")
    print(f"📊 Esquemas: {list(manager.schemas.keys())}")
    print(f"📝 Campos en transactions_collection: {len(manager.schemas['transactions_collection'].fields)}") 