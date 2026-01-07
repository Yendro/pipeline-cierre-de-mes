# Month-End Sales Pipeline

## Install

### Clone repository

```bash
git clone [URL-del-repositorio]
cd pipeline-cierre-de-mes
```

### Create venv

```bash
python -m venv .venv
.venv/bin/activate
```

### Dependencies

```bash
pip install -r requirements.txt

OR

pip install pandas openpyxl
```

## Project Structure

```bash
pipeline-cierre-de-mes/
├── pipeline.py                     # Script
├── requirements.txt                # Python dependencies
├── README.md
├── .gitignore
├── LICENSE
├── transformacion_errores.log      # Error log (automatically generated)
├── data/
│   ├── reporte-cierre-de-mes.csv                           # Input file
│   ├── reporte-cierre-de-mes-transformado-MM-YYYY.xlsx     # Output file
│   └── Ventas-CierreDeMes.sql                              # BigQuery SQL file
└──
```

## Add new elements to map static variables

### Add developments

Tutorial

```python
MAPEO_DESARROLLOS = {
    'Development_name': {                           # New item
        'equivalencia_base': 'key_name',            # e.g.: ABC DE
        'desarrollo_maestro': 'UPPER_KEY_NAME',     # e.g.: DEVELOPMENT
        'formato': 'base_espacio_numero'            # solo_base, base_numero
    }
}
```

### Add special stages

Para algunos desarrollos (como "Puerto Telchac"), existe un mapeo especial que convierte nombres de etapas a números (ej: "Coral" → "20").

```python
MAPEO_ETAPAS_ESPECIALES = {
    'Special_development_name': {
        'etapa1': "11",
        'etapa2': '12'
    }
}
```

### Add column formatting

Tutorial

```python
MAPEO_FORMATOS_COLUMNAS = {
    # Dates
    'Colum_date': 'DD/MM/YYYY',

    # Number
    'Colum_number': '#,##0.00',

    # Coin
    'Colum_coin': '"$"#,##0.00',

    # Percentage
    'Colum_percentage': '0.00%',

    # Text
    'Colum_text': '@'
}
```

### Debugging

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
)
```
