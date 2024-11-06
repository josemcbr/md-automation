## Automatización ficheros

En este script se detalla la configuración y uso del script para generar todos los ficheros de configuración para HSU.

### Instalación y preparación del entorno

1. Clonar el repositorio en la ruta deseada:

```bash
git clone https://github.com/josemcbr/md-automation.git
```

2. Instalar las dependencias

```bash
pip install -r requirements.txt
```

### Características

Este script se encarga de generar los siguientes ficheros:
- Ficheros para los **dmstask**
- Ficheros con las **tablas de gobierno**
- Ficheros de reglas para **data quality**

### Configuración

En la carpeta cfg tenemos dos subdirectorios:
- configs: Ficheros de configuración para el funcionamiento del programa. Dentro de esta carpeta tenemos un fichero **config.cfg** con todos los parametros de configuración para el programa.
- params: Ficheros de parametria para la generación de los distintos ficheros. Dentro de esta carpeta tenemos el fichero **master_fields.csv** dónde tenemos un maestro con todos los campos posibles para cada una de las tablas así como información relevante de los mismos.

Dentro del fichero **config.cfg** hay que destacar varios aspectos:
- Sección **Folders**: Parametrización de todas las rutas de input y output para el programa
- Sección **dmstask**: Parametros para la generación de los dmstask
- Sección **government**: Parametros para la generación de los ficheros con las tablas de gobierno
- Sección **dataquality**: Parámetros para la generación de los ficheros de dataquality

Hay que tener en cuenta que el programa genera la estructura de directorios para las salidas en la ejecución del programa. Es importante que la ruta de entrada se genere antes de la ejecución del programa.

### Consideraciones

Para la lectura de los excel de cada linaje hay que realizar un pequeño tratamiento manual antes de realizar la ejecución del programa:
- Hay que limpiar la columna de nombre de campo de cosas raras que pueda haber. Por ejemplo: si un campo esta tachado eliminarlo del excel, nombres de campo con caracteres especiales.
- Generar una columna con el nombre **valores formateados** dónde tenga el valor del campo "LANDING - Valores" formateado de manera que cada uno de los valores que pueda tomar el campo separado por "," (carácer coma).
- Dentro de la carpeta input puede haber mas de un excel de linaje por legado, el programa tomará la ultima versión que haya del mismo, por este motivo es importante que el excel del linaje mantenga el formato de nombre original (HSU_legado_Linaje_de_datos vXX.X)

### Estructura
| cfg
| - configs: Ficheros de configuración
| - params: Ficheros de parametrías
| Functions
| - generic_functions.py: funciones genericas para el programa
| - dmstask_functions.py: funciones para la generación de los dmstasks
| - government_tables_functions.py: funcioens para la generación de las tablas de gobierno
| - dataqwality_functions.py: funcioens para la generacion de los dataqualitys
| main.py: modulo principal del programa
| config.py: modulo que crea y carga la configuración
| logger.py: modulo que crea el logger
| requiremnts.txt: dependencias

### Ejecución

Para ejecutar el script hay que ejecutar la siguiente sentencia:

```bash
python main --legado legado
```

Dónde ***"legado"*** será obligatorió y tendrá un valor de entre los distintos legados disponibles

Una vez qwe se haya ejecutado el programa dejará los ficheros en las rutas definidas para las partes que esten activas en el config.
