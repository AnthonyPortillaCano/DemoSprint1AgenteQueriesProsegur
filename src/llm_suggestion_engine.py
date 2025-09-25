"""
🎯 MOTOR DE SUGERENCIAS LLM - AZURE OPENAI (usando openai>=1.0.0)
"""

import os
import openai
from typing import Optional, Dict, Any

class LLMSuggestionEngine:
    """
    🧠 MOTOR DE SUGERENCIAS INTELIGENTES
    Utiliza Azure OpenAI para proporcionar sugerencias de mejora para queries en lenguaje natural con control de costos.
    """
    def __init__(self, api_key: Optional[str] = None, endpoint: Optional[str] = None, deployment: Optional[str] = None):
        self.api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.endpoint = endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
        self.deployment = deployment or os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2023-05-15")
        if self.api_key and self.endpoint:
            self.client = openai.AzureOpenAI(
                api_key=self.api_key,
                api_version=self.api_version,
                azure_endpoint=self.endpoint
            )
        else:
            self.client = None

    def suggest_query_improvement(self, natural_text: str, current_result: Optional[str] = None) -> Dict[str, Any]:
        if not self.client or not self.deployment:
            return {
                "suggestions": "LLM no disponible - verificar configuración de Azure OpenAI",
                "model_used": None,
                "tokens_used": 0,
                "cost_estimate": 0.0
            }
        try:
            system_prompt = (
                "Eres un experto en MongoDB y consultas en lenguaje natural. "
                "Tu tarea es sugerir mejoras para hacer las consultas más claras, precisas y eficientes. "
                "Considera: Claridad, terminología MongoDB, optimización y mejores prácticas. "
                "Responde de forma concisa y práctica."
            )
            user_prompt = f"Consulta original: {natural_text}"
            if current_result:
                user_prompt += f"\nResultado actual: {current_result}"
            user_prompt += "\n\nSugiere mejoras específicas para esta consulta:"

            response = self.client.chat.completions.create(
                model=self.deployment,  # Nombre del deployment/modelo en Azure
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=300,
                temperature=0.3
            )
            tokens_used = response.usage.total_tokens if hasattr(response, 'usage') else None
            return {
                "suggestions": response.choices[0].message.content,
                "model_used": self.deployment,
                "tokens_used": tokens_used,
                "cost_estimate": None
            }
        except Exception as e:
            return {
                "suggestions": f"Error generando sugerencias: {str(e)}",
                "model_used": None,
                "tokens_used": 0,
                "cost_estimate": 0.0
            }

    def suggest_field_mapping(self, unknown_field: str, available_fields: list) -> Dict[str, Any]:
        if not self.client or not self.deployment:
            return {
                "suggestions": "LLM no disponible para sugerencias de campos",
                "model_used": None,
                "tokens_used": 0,
                "cost_estimate": 0.0
            }
        try:
            system_prompt = (
                "Eres un experto en MongoDB. Ayuda a mapear campos desconocidos "
                "a campos disponibles basándote en similitudes semánticas y patrones comunes."
            )
            user_prompt = (
                f"Campo desconocido: '{unknown_field}'\n"
                f"Campos disponibles: {', '.join(available_fields)}\n"
                "Sugiere los 3 campos más similares y explica por qué:"
            )
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=200,
                temperature=0.2
            )
            tokens_used = response.usage.total_tokens if hasattr(response, 'usage') else None
            return {
                "suggestions": response.choices[0].message.content,
                "model_used": self.deployment,
                "tokens_used": tokens_used,
                "cost_estimate": None
            }
        except Exception as e:
            return {
                "suggestions": f"Error en sugerencias de campos: {str(e)}",
                "model_used": None,
                "tokens_used": 0,
                "cost_estimate": 0.0
            }

    def get_usage_stats(self) -> Dict[str, Any]:
        return {
            "llm_available": bool(self.client and self.deployment),
            "model": self.deployment if (self.client and self.deployment) else None,
            "endpoint": self.endpoint if (self.client and self.deployment) else None
        } 