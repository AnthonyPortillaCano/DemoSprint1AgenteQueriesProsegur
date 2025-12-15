# Principios de Diseño y Relación con el Estado del Arte

Este agente de generación de queries MongoDB implementa principios inspirados en dos trabajos clave del estado del arte:

## 1. SmBoP (2021) — Parsing semántico bottom-up semi-autoregresivo
- **Modularidad y construcción progresiva:** El agente arma el pipeline de MongoDB paso a paso, agregando etapas como `$match`, `$group`, `$project`, `$unwind`, etc., según la instrucción.
- **Soporte para estructuras anidadas:** Permite generar pipelines complejos con operadores como `$concat`, `$project`, `$cond`, necesarios para reportes avanzados.
- **Uso del esquema:** Utiliza el esquema de las colecciones y campos para validar y sugerir campos, asegurando consultas válidas y relevantes.
- **No genera el pipeline completo de una sola vez:** El pipeline se construye por etapas, validando cada bloque, similar al enfoque bottom-up de SmBoP.

## 2. Bridging the Gap (2025) — Traducción directa NL → MongoDB
- **Traducción directa de lenguaje natural a pipeline MongoDB:** El agente traduce frases simples directamente a etapas de pipeline como `$match`, `$group`, `$unwind`, `$project`, y operadores como `$sum`, `$count`, `$avg`, etc.
- **Cobertura de operadores y bloques planos:** Cubre la mayoría de los casos de uso comunes en MongoDB, especialmente para instrucciones simples y directas.
- **Compatibilidad con MongoDB real:** El pipeline generado es ejecutable en instancias reales de MongoDB y se adapta a esquemas reales.
- **Limitaciones en lógica condicional compleja:** Al igual que el paper, el agente puede tener limitaciones para lógica condicional muy avanzada, aunque implementa patrones para casos frecuentes.

## Resumen
- El agente aplica principios de ambos papers: modularidad, bottom-up, uso de esquema y generación progresiva (SmBoP); traducción directa NL→MongoDB, cobertura de operadores básicos y compatibilidad real (Bridging the Gap).
- No implementa los modelos neuronales exactos, pero sí sus ideas prácticas y de ingeniería para la generación de pipelines MongoDB.

---

**Referencia rápida:**
- SmBoP: Semantic Parsing with Semi-Autoregressive Bottom-Up Decoding (Rubin & Berant, 2021)
- Bridging the Gap: Direct NL → MongoDB Query Translation for Real-World Data (2025)
