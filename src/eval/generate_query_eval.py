import json
import time
import datetime
import re
import argparse
import os
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(project_root / 'src'))

try:
    from AgenteGeneradorQueryMongo import SmartMongoQueryGenerator
except Exception as e:
    # allow running even if class import needs adjustments; will raise later if used
    SmartMongoQueryGenerator = None


def load_gold(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def safe_parse_json(s):
    try:
        return json.loads(s), None
    except Exception as e:
        return None, str(e)


def fuzzy_map_pipeline_fields(pipeline, sample_fields, threshold=0.75):
    # Map keys in $project/$group and $field references to best match in sample_fields
    from difflib import SequenceMatcher
    def best_match(field):
        field_norm = re.sub(r'[^a-z0-9]', '', str(field).lower())
        best = None
        best_score = 0.0
        for sf in sample_fields:
            sf_norm = re.sub(r'[^a-z0-9]', '', str(sf).lower())
            score = SequenceMatcher(None, field_norm, sf_norm).ratio()
            if score > best_score:
                best = sf
                best_score = score
        return best if best_score >= threshold else field
    def walk(obj):
        if isinstance(obj, dict):
            new = {}
            for k, v in obj.items():
                newk = best_match(k)
                newv = walk(v)
                if isinstance(v, str) and v.startswith('$'):
                    fld = v.lstrip('$')
                    mapped = best_match(fld)
                    newv = f"${mapped}" if mapped else v
                new[newk] = newv
            return new
        elif isinstance(obj, list):
            return [walk(x) for x in obj]
        else:
            return obj
    return walk(pipeline)

def evaluate(agent, cases, use_mongomock=True, mode='strict'):
    # Collect all possible fields from sample docs for fuzzy mapping
    sample_docs = {
        'ventas': [
            {'producto': 'A', 'cantidad': 10, 'fecha': datetime.datetime(2024,3,1), 'total': 100.0},
            {'producto': 'B', 'cantidad': 5, 'fecha': datetime.datetime(2024,3,2), 'total': 50.0},
            {'producto': 'A', 'cantidad': 7, 'fecha': datetime.datetime(2024,6,5), 'total': 70.0}
        ],
        'clientes': [
            {'nombre': 'Jose', 'ciudad': 'Lima'},
            {'nombre': 'Joan', 'ciudad': 'Cusco'},
            {'nombre': 'Joanna', 'ciudad': 'Lima'}
        ],
        'empleados': [
            {'nombre': 'Ana', 'departamento': 'Ventas'},
            {'nombre': 'Luis', 'departamento': 'TI'},
            {'nombre': 'Carlos', 'departamento': 'Ventas'}
        ],
        'productos': [
            {'producto': 'A', 'precio': 10.0},
            {'producto': 'B', 'precio': 20.0},
            {'producto': 'C', 'precio': 15.5}
        ]
    }
    all_fields = set()
    for docs in sample_docs.values():
        for doc in docs:
            all_fields.update(doc.keys())

    def extract_fields(pipeline):
        # Recursively extract all field names from pipeline dict/list
        fields = set()
        if isinstance(pipeline, dict):
            for k, v in pipeline.items():
                fields.add(k)
                fields.update(extract_fields(v))
        elif isinstance(pipeline, list):
            for item in pipeline:
                fields.update(extract_fields(item))
        return fields

    results = []
    for case in cases:
        nl = case.get('nl')
        coll = case.get('collection')
        expected = case.get('expected_pipeline')
        start = time.time()
        try:
            gen = agent.generate_query(coll, nl)
        except Exception as e:
            gen = None
            gen_err = str(e)
        else:
            gen_err = None
        latency_ms = (time.time() - start) * 1000

        # Accept multiple return types from agent.generate_query:
        # - already a Python list/dict (preferred)
        # - JSON string
        # - other reprs (attempt best-effort)
        if not gen:
            parsed = None
            parse_err = gen_err or 'no output'
        else:
            if isinstance(gen, (list, dict)):
                parsed = gen
                parse_err = None
            elif isinstance(gen, str):
                parsed, parse_err = safe_parse_json(gen)
            else:
                # Try to coerce via JSON on str() as a last resort
                try:
                    parsed = json.loads(str(gen))
                    parse_err = None
                except Exception as e:
                    parsed = None
                    parse_err = f'unhandled return type {type(gen)}: {e}'

        # If mode is 'mapped', apply fuzzy mapping to pipeline fields
        mapped_pipeline = None
        mapping_applied = None
        if parsed and mode == 'mapped':
            mapped_pipeline = fuzzy_map_pipeline_fields(parsed, all_fields, threshold=0.75)
            mapping_applied = True
        else:
            mapped_pipeline = parsed
            mapping_applied = False


        expected_fields = extract_fields(expected)
        gen_fields = extract_fields(mapped_pipeline) if mapped_pipeline else set()

        tp = len(expected_fields & gen_fields)
        fp = len(gen_fields - expected_fields)
        fn = len(expected_fields - gen_fields)
        precision = tp / (tp + fp) if (tp + fp) > 0 else None
        recall = tp / (tp + fn) if (tp + fn) > 0 else None
        f1 = (2 * precision * recall / (precision + recall)) if precision and recall and (precision + recall) > 0 else None

        # Execution success and functional equivalence are tested with mongomock if requested
        exec_success = None
        func_equiv = None
        exec_error = None
        syntactic = mapped_pipeline is not None
        if use_mongomock:
            try:
                import mongomock
                # Create a small in-memory DB and collections with minimal docs if needed
                client = mongomock.MongoClient()
                db = client['testdb']
                # Helper to parse ISO date strings to datetime
                def _parse_date(s):
                    if isinstance(s, str):
                        try:
                            # Try YYYY-MM-DD or full ISO
                            if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
                                return datetime.datetime.strptime(s, "%Y-%m-%d")
                            return datetime.datetime.fromisoformat(s)
                        except Exception:
                            return s
                    return s

                # Populate minimal documents for collection names found in cases (best-effort)
                # Use correct Python types: datetime for fecha, ints/floats for numeric fields
                sample_docs = {
                    'ventas': [
                        {'producto': 'A', 'cantidad': 10, 'fecha': datetime.datetime(2024,3,1), 'total': 100.0},
                        {'producto': 'B', 'cantidad': 5, 'fecha': datetime.datetime(2024,3,2), 'total': 50.0},
                        {'producto': 'A', 'cantidad': 7, 'fecha': datetime.datetime(2024,6,5), 'total': 70.0}
                    ],
                    'clientes': [
                        {'nombre': 'Jose', 'ciudad': 'Lima'},
                        {'nombre': 'Joan', 'ciudad': 'Cusco'},
                        {'nombre': 'Joanna', 'ciudad': 'Lima'}
                    ],
                    'empleados': [
                        {'nombre': 'Ana', 'departamento': 'Ventas'},
                        {'nombre': 'Luis', 'departamento': 'TI'},
                        {'nombre': 'Carlos', 'departamento': 'Ventas'}
                    ],
                    'productos': [
                        {'producto': 'A', 'precio': 10.0},
                        {'producto': 'B', 'precio': 20.0},
                        {'producto': 'C', 'precio': 15.5}
                    ]
                }
                for cname, docs in sample_docs.items():
                    if cname not in db.list_collection_names():
                        db[cname].insert_many(docs)

                # Before executing pipelines, coerce any ISO date strings inside pipeline dicts to datetime
                def _coerce_dates_in_obj(obj):
                    if isinstance(obj, dict):
                        new = {}
                        for k, v in obj.items():
                            if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}(T.*)?$", v):
                                new[k] = _parse_date(v)
                            else:
                                new[k] = _coerce_dates_in_obj(v)
                        return new
                    elif isinstance(obj, list):
                        return [_coerce_dates_in_obj(i) for i in obj]
                    else:
                        return obj

                # Try to execute both pipelines (expected and generated) and compare result shapes
                exec_success = False
                if syntactic:
                    try:
                        # Coerce date-like strings in pipelines to datetime objects to match sample_docs types
                        parsed_exec = _coerce_dates_in_obj(mapped_pipeline)
                        expected_exec = _coerce_dates_in_obj(expected)
                        gen_res = list(db[coll].aggregate(parsed_exec))
                        exp_res = list(db[coll].aggregate(expected_exec))
                        exec_success = True
                        # Normalize results to JSON-serializable forms (ObjectId -> str, datetime -> iso)
                        def _make_jsonable(obj):
                            try:
                                from bson import ObjectId
                            except ImportError:
                                ObjectId = None
                            if isinstance(obj, dict):
                                return {k: _make_jsonable(v) for k, v in obj.items()}
                            elif isinstance(obj, list):
                                return [_make_jsonable(v) for v in obj]
                            elif ObjectId is not None and isinstance(obj, ObjectId):
                                return str(obj)
                            elif isinstance(obj, datetime.datetime):
                                return obj.isoformat()
                            else:
                                return obj

                        gen_norm = _make_jsonable(gen_res)
                        exp_norm = _make_jsonable(exp_res)
                        # Simple functional equivalence: compare sorted json dumps (best-effort)
                        func_equiv = (json.dumps(gen_norm, sort_keys=True, ensure_ascii=False) == json.dumps(exp_norm, sort_keys=True, ensure_ascii=False))
                    except Exception as e:
                        exec_success = False
                        func_equiv = False
                        exec_error = str(e)
                else:
                    exec_error = 'syntactic invalid, skipped'
            except Exception as e:
                exec_success = None
                func_equiv = None
                exec_error = str(e)

        if exec_error:
            print(f"Case {case.get('id')}: exec_error={exec_error}")

        results.append({
            'id': case.get('id'),
            'nl': nl,
            'collection': coll,
            'syntactic_valid': bool(syntactic),
            'parse_error': parse_err,
            'gen_error': gen_err,
            'latency_ms': latency_ms,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'exec_success': exec_success,
            'func_equiv': func_equiv,
            'exec_error': exec_error,
            'generated_pipeline': parsed,
            'mapped_pipeline': mapped_pipeline if mapping_applied else None,
            'expected_pipeline': expected,
            'mapping_applied': mapping_applied
        })

        # Handle bson import gracefully
        try:
            from bson import ObjectId
        except ImportError:
            ObjectId = None

    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--gold', default=str(project_root / 'datasets' / 'eval_queries.json'))
    p.add_argument('--out', default=str(project_root / 'results' / 'eval_result.json'))
    p.add_argument('--mongomock', action='store_true', default=True)
    p.add_argument('--mode', choices=['strict', 'mapped'], default='strict',
                   help='Modo de evaluación: strict (sin mapeo) o mapped (con mapeo fuzzy de campos).')
    args = p.parse_args()

    cases = load_gold(args.gold)

    if SmartMongoQueryGenerator is None:
        print('No se pudo importar SmartMongoQueryGenerator desde src; asegúrate de que el path y el módulo sean correctos.')
        sys.exit(1)



    agent = SmartMongoQueryGenerator()

    # Custom JSON encoder for ObjectId
    try:
        from bson import ObjectId
    except ImportError:
        ObjectId = None

    class JSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if ObjectId is not None and isinstance(obj, ObjectId):
                return str(obj)
            return super().default(obj)

    # Strict mode
    results_strict = evaluate(agent, cases, use_mongomock=args.mongomock, mode='strict')
    out_path_strict = Path(args.out).with_name('eval_result_strict.json')
    out_path_strict.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path_strict, 'w', encoding='utf-8') as f:
        json.dump(results_strict, f, indent=2, ensure_ascii=False, cls=JSONEncoder)

    # Mapped mode
    results_mapped = evaluate(agent, cases, use_mongomock=args.mongomock, mode='mapped')
    out_path_mapped = Path(args.out).with_name('eval_result_mapped.json')
    with open(out_path_mapped, 'w', encoding='utf-8') as f:
        json.dump(results_mapped, f, indent=2, ensure_ascii=False, cls=JSONEncoder)

    # Print quick summary for both modes
    for label, results in [('Strict', results_strict), ('Mapped', results_mapped)]:
        total = len(results)
        syntactic = sum(1 for r in results if r['syntactic_valid'])
        exec_success = sum(1 for r in results if r['exec_success'])
        func_equiv = sum(1 for r in results if r['func_equiv'])
        avg_f1 = sum(r['f1'] for r in results if r['f1'] is not None) / max(1, sum(1 for r in results if r['f1'] is not None))
        avg_latency = sum(r['latency_ms'] for r in results) / max(1, total)
        print(f"[{label}] Evaluados: {total}")
        print(f"[{label}] Syntactic valid: {syntactic}/{total}")
        print(f"[{label}] Exec success (mongomock): {exec_success}/{total}")
        print(f"[{label}] Func equiv: {func_equiv}/{total}")
        print(f"[{label}] Avg field-F1 (where available): {avg_f1:.3f}")
        print(f"[{label}] Avg latency ms: {avg_latency:.1f}")


if __name__ == '__main__':
    main()