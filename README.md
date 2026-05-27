# Pipeline de Datos — Brazilian E-Commerce Olist

**Parcial Final · Ingeniería de Datos**

## Cómo ejecutar en Google Colab

1. Abre [Google Colab](https://colab.research.google.com)
2. `Archivo → Abrir cuaderno → GitHub` y pega la URL de este repo, o sube el `.ipynb` manualmente
3. Sigue las instrucciones de la **Sección 0** del notebook:
   - Ve a [kaggle.com](https://www.kaggle.com) → tu foto → **Settings** → **API** → **Create New Token**
   - Descarga el archivo `kaggle.json`
   - Ejecúta la celda de subida y carga el archivo cuando se solicite
4. Ejecuta todas las celdas en orden (`Runtime → Run all`)

## Estructura del proyecto

```
PARCIAL-ING-DATOS/
├── Pipeline_Ecommerce_Olist.ipynb   ← Notebook principal
├── data/
│   ├── raw/          ← CSVs descargados de Kaggle (se generan al ejecutar)
│   ├── processed/    ← Parquet + SQLite (se generan al ejecutar)
│   └── exports/      ← Gráficos PNG + CSVs para Power BI
└── README.md
```

## Secciones del notebook

| # | Sección | Criterio |
|---|---|---|
| 0 | Configuración y descarga | — |
| 1 | Ingesta de datos | Criterio 1 (20%) |
| 2 | Exploración inicial | Criterio 1 (20%) |
| 3 | Limpieza de datos | Criterio 2 (20%) |
| 4 | Transformación y Feature Engineering | Criterio 2 (20%) |
| 5 | Almacenamiento (Parquet + SQLite) | Criterio 3 (20%) |
| 6 | Diseño del pipeline | Criterio 3 (20%) |
| 7 | Análisis Exploratorio (12+ análisis) | Criterio 4 (20%) |
| 8 | Visualizaciones (13 gráficos) | Criterio 4 (20%) |
| 9 | Conclusiones e Insights | Criterio 5 (20%) |
| 10 | Documentación técnica | Criterio 5 (20%) |

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — ~100.000 órdenes reales, 9 tablas relacionadas.
