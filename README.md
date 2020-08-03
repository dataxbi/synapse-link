# Seguimiento a una flota de vehículos con Cosmos DB,  Synapse y Power BI

Este repositorio contiene el código utilizado en la entrada de blog https://www.dataxbi.com/blog/2020/08/03/flota-vehiculos-cosmos-db-synapse-power-bi

 [![Cosmoss DB - Synapse - Power BI](synapse-link.jpg)](https://www.dataxbi.com/blog/2020/08/03/flota-vehiculos-cosmos-db-synapse-power-bi)


- Script Python para las simulación de una flota de vehículos (vehicle_fleet_simulator.py)
- Notebooks utilizados en Synapse Studio

 ## Simulación de una flota de vehículos (vehicle_fleet_simulator.py)

 Simula la operación de una flota de vehículos y envía actualizaciones del estado de los vehículo a Azure Cosmos DB.

Los vehículos pueden estar parados o viajando y antes de comenzar un viaje se cargan las mercancías y al finalizar un viaje se descargan.

Estos son los identificadores de los estados por los que puede pasar un vehículo:
- stopped
- load-started
- load-finished
- trip-started
- trip-otw  
- trip-finished
- unload-started
- unload-finished

El identificador trip-otw indica que el vehículo está viajando (on the way).

Este es un ejemplo de los datos que se envían a Cosmos DB durante un viaje:
```
{
        "id": "b00d6b7ac4934991aca6c3c67bd14e90",
        "plate_number": "1677 BYO",
        "vehicle_id": "b099829a39c3468daabd48ec4360571c",
        "status": "trip-otw",
        "timestamp": "2020-06-22 21:19:51.548",
        "kms": 5,
        "trip_id": "5feea2a1768c4ec0807d1a909f8530d7",
        "trip_name": "Trip 2000"
    }
```

En la simulación, primero se crea la flota de vehículos con la cantidad definida en la variable `TOTAL_VEHICLES` y se introducen los vehículos en una cola.
Para cada vehículo se genera un identificador y un número de matrícula aleatorios.

Despúes se crean los viajes definidos en la variable `TOTAL_TRIPS`.
Cada viaje tiene un identificador, un nombre y una cantidad de kms, que se generan de forma aleatoria.

A continuación se comienza a asignar los viajes a los vehículos disponibles en la cola y se programa una tarea asincrónica para simular cada viaje.
Si se vacía la cola pero aún hay viajes que asignar, se monitorea la cola hasta que haya vehículos disponibles de nuevo.
Cada viaje comenzará en un tiempo aleatorio entre 0 y 120 segundos y al terminar el viaje, el vehículo se envía de nuevo a la cola.

Para la simulación de cada viaje se utilizan valores aleatorios para los tiempo de carga y descarga y la velocidad del vehiculo.

Durante un viaje se envían actualizaciones a Cosmos DB vez que hay un cambio de estado y mientras el vehículo está viajando (estado trip-otw) se envía 
una actualización cada x segundos (definidos en la variable `TRIP_UPDATE_INTERVAL`).


Para la comunicación con Cosmos DB se utiliza el paquete [azure-cosmos](https://pypi.org/project/azure-cosmos/) de Microsoft y los parámetros de conexión se guardan en las variables:
- COSMOS_DB_ENDPOINT
- COSMOS_DB_KEY
- COSMOS_DB_DATABASE_NAME
- COSMOS_DB_CONTAINER_NAME
