# DataHub
En el proyecto se muestra un ejemplo de uso de DataHub sobre la base de datos open-source _northwind_.

Para poder desarrollar el proyecto se deben seguir los pasos para la instalación de DataHub que se pueden consultar en https://datahubproject.io/docs/quickstart.

En este caso práctico supondremos que el marco de datos de la empresa consta de:
- Origen _MySQL_ con la base de datos _northwind_.
- _Data lake PostgreSQL_ con el modelado de la base de datos original en _Data Vault_ y el modelo en estrella orientado al análisis.

Una vez realizada la instalación de DataHub, los _dockerfiles_ de _MySQL_ y _PostgreSQL_ permitirán crear dos contenedores, cada uno con su respectiva base de datos, en la que se podrán insertar los datos por medio de sus _scripts_ asociados.

Por su parte, en el script get_column_lineages.py se muestra el código mencionado en la memoria para producir el linaje de columnas entre las tablas de la base de datos original y el modelado en Data Vault.
