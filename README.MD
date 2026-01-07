# Pipeline de ventas de fin de mes

## Instalación

### Clonar repositorio

```bash
git clone [URL-del-repositorio]
cd pipeline-cierre-de-mes
```

### Crear venv

```bash
python -m venv .venv
.venv/bin/activate
```

### Dependencias

```bash
pip install -r requirements.txt

OR

pip install pandas openpyxl
```

## Estructura del proyecto

```bash
pipeline-cierre-de-mes/
├── pipeline.py                     # Script
├── requirements.txt                # Dependencias de Python
├── README.md
├── .gitignore
├── LICENSE
├── transformacion_errores.log      # Log de errores e información (generado automáticamente)
├── data/
│   ├── reporte-cierre-de-mes.csv                           # Archivo de entrada
│   ├── reporte-cierre-de-mes-transformado-MM-YYYY.xlsx     # Archivo de salida
│   └── Ventas-CierreDeMes.sql                              # Consulta SQL de BigQuery
└──
```

## Agregar nuevos elementos a las variables estaticas

### Agregar elementos al mapeo de desarrollos

El mapeo de desarrollos sirve para poder generar los datos en **[Desarrollos_Maestro]** y **[Equivalencia]**, los cuales son una versión para ubicar de forma más directa los desarrollos.  
Para agregar un nuevo desarrollo, se debe de primero escribir el item con el nombre de desarrollo justo como sale en la columna **[Desarrollo]**.

El script contiene un diccionario extenso (`MAPEO_DESARROLLOS`) que define:

- `'equivalencia_base'`: Esta es la clave de texto que se muestra en los datos de la columna **[Equivalencia]**. Es una clave que puede concatenar el _número_ que porían contener algunos datos en la columna **[Etapa]**.

- `'desarrollo_maestro'`: Es el nombre estandarizado de los desarrollos. Se usa para identificar desarrollos de una misma zona.

- `'formato'`: Es el patrón para generar la equivalencia completa. Esto determina la forma en la que se van a concatenar los datos en `'equivalencia_base'` y los datos en **[Etapa]**.

Ejemplo de nuevo desarrollo:

```python
MAPEO_DESARROLLOS = {
    'Nombre_Desarrollo': {
        'equivalencia_base': 'CLAVE',        # e.g.: ABC DE
        'desarrollo_maestro': 'NOMBRE_MAYUSCULAS',
        'formato': 'base_espacio_numero'     # solo_base, base_numero
    }
}
df['Etapa'] = 'E123'
```

> El resultado de esto sería:

| Etapa | Desarrollo_Maestro | Equivalencia |
| ----- | ------------------ | ------------ |
| E123  | NOMBRE_MAYUSCULAS  | CLAVE 123    |

El `'formato'` puede tener los valores `'base_numero'` y `'solo_base'` también. Usando el ejemplo anterior:

**[Equivalencia]**  
`'base_numero' = CLAVE123`  
`'solo_base' = CLAVE`

### Agregar desarrollos con formatos especiales

especial_playaviva: Caso especial para Playaviva Apartments

Mapeo Especial de Etapas
Para algunos desarrollos (como "Puerto Telchac"), existe un mapeo especial que convierte nombres de etapas a números (ej: "Coral" → "20").

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
