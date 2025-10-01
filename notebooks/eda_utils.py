import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def describe_transactions(data):
    df = pd.DataFrame(data)
def analyze_missing_and_outliers(transactions_list):
    """
    Analiza valores nulos y outliers en las columnas numéricas del dataset de transacciones.
    Muestra un resumen de nulos y boxplots para detectar outliers.
    """
    df = pd.DataFrame(transactions_list)
    print("Resumen de valores nulos por columna:")
    print(df.isnull().sum())

    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if not numeric_cols:
        print("No hay columnas numéricas para analizar outliers.")
        return
    print("Boxplots para detección de outliers en columnas numéricas:")
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df[numeric_cols])
    plt.xticks(rotation=45)
    plt.title("Boxplots de columnas numéricas")
    plt.show()
    desc = df.describe(include='all')
    return desc

def plot_key_distributions(data, key_fields):
    df = pd.DataFrame(data)
    for field in key_fields:
        plt.figure(figsize=(8,4))
        sns.histplot(df[field].dropna(), kde=True)
        plt.title(f'Distribución de {field}')
        plt.xlabel(field)
        plt.ylabel('Frecuencia')
        plt.show()

def plot_amount_boxplot(data, amount_field):
    df = pd.DataFrame(data)
    plt.figure(figsize=(6,4))
    sns.boxplot(x=df[amount_field].dropna())
    plt.title(f'Boxplot de {amount_field}')
    plt.xlabel(amount_field)
    plt.show()
