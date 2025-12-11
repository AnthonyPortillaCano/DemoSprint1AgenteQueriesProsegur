import streamlit as st
import requests
import re

st.title("Generador de Queries MongoDB con LLM")

st.markdown("""
Este app te permite generar queries MongoDB desde lenguaje natural.
""")


#API_URL = "http://localhost:8000/assist/"
API_URL = "https://apppythonnek-graufac6ducma6ee.eastus-01.azurewebsites.net/assist/"

# Entrada del usuario solo para la instrucción
natural_text = st.text_area("Instrucción en lenguaje natural", value="crear campo user")

if st.button("Generar Query"):
    with st.spinner("Generando..."):
        try:
            response = requests.post(
                API_URL,
                json={"natural_text": natural_text}
            )
            data = response.json()
            query = data.get("query", "")
            # Si query es una lista, conviértela a string legible
            if isinstance(query, list):
                import json
                query = json.dumps(query, indent=2, ensure_ascii=False)
            suggestions = data.get("suggestions", "")
            show_llm_as_query = False
            # Mostrar la query generada aunque la colección no sea 'labs'
            if query.strip() == "" or re.search(r'aggregate\(\[\s*\]\)', query):
                show_llm_as_query = True
            # Si la query es el dummy de campo_no_encontrado, mostrar mensaje amigable
            if 'campo_no_encontrado' in query:
                st.subheader("No se pudo encontrar el/los campo(s) solicitado(s)")
                st.info("La instrucción no coincide con ningún campo conocido en la colección. Por favor, revisa el nombre del campo o intenta con otra instrucción.")
            elif show_llm_as_query and suggestions:
                st.subheader("Query generada (por LLM):")
                st.code(suggestions, language="python")
            else:
                st.subheader("Query generada:")
                st.code(query, language="python")
            if suggestions and not show_llm_as_query:
                st.subheader("Sugerencias del LLM:")
                st.write(suggestions)
            if "error" in data:
                st.error(data["error"])
        except Exception as e:
            st.error(f"Error al conectar con la API: {e}") 