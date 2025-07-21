import streamlit as st
import requests

st.title("Generador de Queries MongoDB con LLM")

st.markdown("""
Este frontend te permite generar queries MongoDB desde lenguaje natural usando tu API.
""")

# Cambia esta URL si tu API está desplegada en Azure o en otro servidor
API_URL = "http://localhost:8000/assist/"

# Entradas del usuario
collection = st.text_input("Nombre de la colección", value="lab")
natural_text = st.text_area("Instrucción en lenguaje natural", value="crear campo user")

if st.button("Generar Query"):
    with st.spinner("Generando..."):
        try:
            response = requests.post(
                API_URL,
                json={"collection": collection, "natural_text": natural_text}
            )
            data = response.json()
            if "query" in data:
                st.subheader("Query generada:")
                st.code(data["query"], language="python")
            if "suggestions" in data:
                st.subheader("Sugerencias del LLM:")
                st.write(data["suggestions"])
            if "error" in data:
                st.error(data["error"])
        except Exception as e:
            st.error(f"Error al conectar con la API: {e}") 