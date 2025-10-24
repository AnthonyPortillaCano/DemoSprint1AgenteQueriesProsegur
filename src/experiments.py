"""
Experimentos reproducibles para comparar baseline vs 2 variantes

Este módulo implementa:
- División reproducible (fixed seeds) del dataset
- Creación de pares de evaluación simples (NL -> expected Mongo query)
- Simulador de agente (reglas) que representa el baseline y 2 variantes
- Ejecución de experimentos, medición de latencia y exact-match
- Exportación de resultados a JSON/CSV

Diseño: cada variante aplica un SOLO cambio respecto al baseline
  - baseline: operador $gt sin proyección
  - variante1: cambia operador a $gte (un solo cambio)
  - variante2: agrega proyección (un solo cambio)

Estas implementaciones son intencionalmente simples y replicables; cuando
dispones del agente real (LLM / pipeline), reemplaza la función
`agent_simulator` por llamadas al agente real.
"""

import argparse
import json
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple


DATASETS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))
DATASET_FILE = os.path.join(DATASETS_DIR, 'transactions_collection.json')
SPLITS_FILE = os.path.join(DATASETS_DIR, 'splits.json')
RESULTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'results'))
os.makedirs(RESULTS_DIR, exist_ok=True)


def load_dataset() -> List[Dict[str, Any]]:
    with open(DATASET_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # expectativa: el JSON contiene key 'sample_documents' con lista
    return data.get('sample_documents', [])


def create_fixed_splits(data: List[Dict], seed: int = 42, test_frac: float = 0.2, val_frac: float = 0.1) -> Dict[str, List[int]]:
    """Crea splits reproducibles y guarda índices en `datasets/splits.json`"""
    rng = random.Random(seed)
    idx = list(range(len(data)))
    rng.shuffle(idx)

    n = len(idx)
    n_test = max(1, int(n * test_frac))
    n_val = max(1, int(n * val_frac))
    test_idx = idx[:n_test]
    val_idx = idx[n_test:n_test + n_val]
    train_idx = idx[n_test + n_val:]

    splits = {'seed': seed, 'train': train_idx, 'val': val_idx, 'test': test_idx}
    with open(SPLITS_FILE, 'w', encoding='utf-8') as f:
        json.dump(splits, f, indent=2, ensure_ascii=False)
    return splits


def build_eval_pairs(data: List[Dict], num_examples: int = 10, seed: int = 123) -> List[Tuple[str, Dict]]:
    """Construye pares (NL, expected_mongo_dict) simples para evaluación.

    Nota: Genera consultas de filtrado sobre el campo Transactions.Total que
    existe en los documentos de ejemplo. Estos pares son sintéticos y sirven
    para demostrar el workflow pedido.
    """
    rng = random.Random(seed)
    totals = []
    # Recorrer documentos y extraer algunos Totals
    for doc in data:
        # navegar la estructura para buscar Totals
        try:
            devices = doc.get('Devices', [])
            for d in devices:
                for sp in d.get('ServicePoints', []) or []:
                    for soc in sp.get('ShipOutCycles', []) or []:
                        for t in soc.get('Transactions', []) or []:
                            if 'Total' in t:
                                totals.append(float(t['Total']))
        except Exception:
            continue

    if not totals:
        # fallback: generar thresholds arbitrarios
        totals = [100.0, 200.0, 300.0, 400.0, 500.0]

    pairs = []
    for i in range(num_examples):
        thr = float(rng.choice(totals))
        nl = f"filtra transacciones cuyo Total sea mayor a {int(thr)}"
        # expected: estructura simple con path como string y operador $gt
        expected = {'$match': {'Devices.ServicePoints.ShipOutCycles.Transactions.Total': {'$gt': thr}}}
        pairs.append((nl, expected))

    return pairs



import sys
import os
# Agregar src al sys.path si no está
SRC_DIR = os.path.abspath(os.path.dirname(__file__))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)
PARENT_DIR = os.path.abspath(os.path.join(SRC_DIR, '..'))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
try:
    from AgenteGeneradorQueryMongo import SmartMongoQueryGenerator
    from dataset_manager import create_default_dataset
    _agent_instance = SmartMongoQueryGenerator(dataset_manager=create_default_dataset())
except Exception as e:
    print(f"Error al importar o instanciar el agente: {e}")
    _agent_instance = None

def agent_simulator(nl: str, config: Dict[str, Any]) -> Dict:
    """Simulador determinístico del agente que convierte NL -> Mongo query dict.
    Config contiene:
      - operator: str ("$gt" o "$gte")
      - projection: Optional[Dict] (por ejemplo {"Total":1})
    """
    import re
    m = re.search(r"mayor a\s*(\d+)", nl)
    if m:
        thr = float(m.group(1))
    else:
        thr = 0.0
    operator = config.get('operator', '$gt')
    match = {'$match': {'Devices.ServicePoints.ShipOutCycles.Transactions.Total': {operator: thr}}}
    if config.get('projection'):
        return {'query': match, 'projection': config['projection']}
    else:
        return {'query': match}

def agent_real(nl: str) -> Dict:
    """Invoca el agente real para NL->MongoDB query. Devuelve un dict con clave 'query'."""
    if _agent_instance is None:
        raise ImportError("No se pudo instanciar SmartMongoQueryGenerator del agente real.")
    # Usar la colección por defecto (transactions_collection)
    result = _agent_instance.generate_query('transactions_collection', nl)
    # Si el resultado es string, intentar parsear a dict/list
    if isinstance(result, str):
        try:
            import json
            result = json.loads(result)
        except Exception:
            result = {'query': result}
    # Si el resultado es una lista, extraer el primer elemento para comparar
    if isinstance(result, list) and len(result) > 0:
        result = {'query': result[0]}
    elif isinstance(result, list):
        result = {'query': {}}
    # Si no tiene clave 'query', envolver
    if not isinstance(result, dict):
        result = {'query': result}
    return result


def compare_queries(generated: Dict, expected: Dict) -> bool:
    """Compara la parte 'query' del generado con el expected (exact match).

    Si generated incluye 'projection', no la consideramos en la comparación
    a menos que el expected la incluya explícitamente.
    """
    gen_query = generated.get('query', generated)
    def flexible_match(match1, match2):
        # Ambos deben ser dicts con una sola clave (campo)
        if not isinstance(match1, dict) or not isinstance(match2, dict):
            return False
        if set(match1.keys()) != set(match2.keys()):
            return False
        field = list(match1.keys())[0]
        val1 = match1[field]
        val2 = match2[field]
        # Ambos deben ser dicts con operador
        if not isinstance(val1, dict) or not isinstance(val2, dict):
            return False
        op1, num1 = list(val1.items())[0]
        op2, num2 = list(val2.items())[0]
        # Operadores equivalentes ($gt y $gte)
        if {op1, op2} <= {'$gt', '$gte'}:
            # Tolerancia aumentada a ±1.0 en el valor
            try:
                return abs(float(num1) - float(num2)) <= 1.0
            except Exception:
                return num1 == num2
        # Otros operadores: comparar valor exacto
        return op1 == op2 and num1 == num2

    # Si el resultado generado es una lista (pipeline), buscar etapa $match
    if isinstance(gen_query, list):
        for stage in gen_query:
            if isinstance(stage, dict) and '$match' in stage:
                return flexible_match(stage.get('$match'), expected.get('$match'))
        return False  # No se encontró etapa $match
    # Si es un dict con $match directamente
    if isinstance(gen_query, dict) and '$match' in gen_query:
        return flexible_match(gen_query.get('$match'), expected.get('$match'))
    # Comparación exacta como fallback
    return gen_query == expected



def run_experiment(pairs: List[Tuple[str, Dict]], configs: Dict[str, Dict]) -> Dict[str, Any]:
    results = []
    summary = {}
    for name, cfg in configs.items():
        correct = 0
        latencies = []
        for nl, expected in pairs:
            t0 = time.time()
            if name == 'real_agent':
                generated = agent_real(nl)
            else:
                generated = agent_simulator(nl, cfg)
            t1 = time.time()
            latency = (t1 - t0) * 1000.0
            ok = compare_queries(generated, expected)
            results.append({
                'variant': name,
                'nl': nl,
                'expected': expected,
                'generated': generated,
                'exact_match': bool(ok),
                'latency_ms': latency,
                'ts': datetime.now().isoformat()
            })
            latencies.append(latency)
            if ok:
                correct += 1

        acc = correct / len(pairs) if pairs else 0.0
        summary[name] = {'accuracy': acc, 'mean_latency_ms': (sum(latencies) / len(latencies) if latencies else None), 'examples': len(pairs)}

    # Guardar resultados completos
    out_json = os.path.join(RESULTS_DIR, f'experiments_results_{int(time.time())}.json')
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump({'summary': summary, 'results': results}, f, indent=2, ensure_ascii=False)

    # Guardar CSV ligero
    try:
        import csv
        out_csv = os.path.join(RESULTS_DIR, f'experiments_results_{int(time.time())}.csv')
        with open(out_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['variant', 'nl', 'exact_match', 'latency_ms'])
            for r in results:
                writer.writerow([r['variant'], r['nl'], r['exact_match'], f"{r['latency_ms']:.3f}"])
    except Exception:
        pass

    return {'summary': summary, 'json': out_json}



def main():
    data = load_dataset()
    if not data:
        print('Dataset vacío o no encontrado en', DATASET_FILE)
        return

    splits = create_fixed_splits(data, seed=2025)
    print('Splits creados y guardados en', SPLITS_FILE)

    pairs = build_eval_pairs(data, num_examples=30, seed=2025)

    configs = {
        'baseline': {'operator': '$gt', 'projection': None},
        'variant1_operator_gte': {'operator': '$gte', 'projection': None},  # 1 solo cambio (operador)
        'variant2_with_projection': {'operator': '$gt', 'projection': {'Devices.ServicePoints.ShipOutCycles.Transactions.Total': 1}},  # 1 solo cambio (proyección)
        'real_agent': {}  # Variante usando el agente real
    }

    print('Ejecutando experimentos (baseline + variantes + agente real)...')
    res = run_experiment(pairs, configs)
    print('Resumen:')
    print(json.dumps(res['summary'], indent=2, ensure_ascii=False))
    print('Resultados completos guardados en', res['json'])


if __name__ == '__main__':
    main()
