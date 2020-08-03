"""
Simula la operación de una flota de vehículos y envía actualizaciones del estado de los vehículo a Azure Cosmos DB.

Los vehículos pueden estar parados o viajando y antes de comenzar un viaje se cargan las mercancías y al finalizar un viaje se descargan.

Estos son los identificadores de los estados por los que puede pasar un vehículo:
    stopped
    load-started
    load-finished
    trip-started
    trip-otw  
    trip-finished
    unload-started
    unload-finished

El identificador trip-otw indica que el vehículo está viajando (on the way).

Este es un ejemplo de los datos que se envían a Cosmos DB durante un viaje:
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

En la simulación, primero se crea la flota de vehículos con la cantidad definida en la variable TOTAL_VEHICLES y se introducen los vehículos en una cola.
Para cada vehículo se genera un identificador y un número de matrícula aleatorios.

Despúes se crean los viajes definidos en la variable TOTAL_TRIPS.
Cada viaje tiene un identificador, un nombre y una cantidad de kms, que se generan de forma aleatoria.

A continuación se comienza a asignar los viajes a los vehículos disponibles en la cola y se programa una tarea asincrónica para simular cada viaje.
Si se vacía la cola pero aún hay viajes que asignar, se monitorea la cola hasta que haya vehículos disponibles de nuevo.
Cada viaje comenzará en un tiempo aleatorio entre 0 y 120 segundos y al terminar el viaje, el vehículo se envía de nuevo a la cola.

Para la simulación de cada viaje se utilizan valores aleatorios para los tiempo de carga y descarga y la velocidad del vehiculo.

Durante un viaje se envían actualizaciones a Cosmos DB vez que hay un cambio de estado y mientras el vehículo está viajando (estado trip-otw) se envía 
una actualización cada x segundos (definidos en la variable TRIP_UPDATE_INTERVAL).

Para la comunicación con Cosmos DB se utiliza el paquete azure-cosmos de Microsoft y los parámetros de conexión se guardan en las variables:
    COSMOS_DB_ENDPOINT
    COSMOS_DB_KEY
    COSMOS_DB_DATABASE_NAME
    COSMOS_DB_CONTAINER_NAME

"""

import threading 

from azure.cosmos import exceptions, CosmosClient, PartitionKey

import secrets
import vehicle_trip as vt

TOTAL_VEHICLES = 100
TOTAL_TRIPS = 200
TRIP_UPDATE_INTERVAL = 5 # en segundos

COSMOS_DB_ENDPOINT = secrets.COSMOS_DB_ENDPOINT
COSMOS_DB_KEY = secrets.COSMOS_DB_KEY
COSMOS_DB_DATABASE_NAME = 'test'
COSMOS_DB_CONTAINER_NAME = 'VehiclesLog'

def create_vehicles_queue(total_vehicles, cosmos_db_logger):
    vehicles_queue = vt.VehiclesQueue()
    random_plate_number = iter(vt.RandomVehiclePlateNumberIterator())
    for _ in range(TOTAL_VEHICLES):
        vehicle = vt.Vehicle(next(random_plate_number),cosmos_db_logger)
        vehicle.log()
        vehicles_queue.enqueue(vehicle)
    return vehicles_queue

def create_trips(total_trips):
    random_kms = iter(vt.RandomIntegerIterator(100, 500))
    trips = []
    for i in range(TOTAL_TRIPS):
        trip_name = 'T-{:0>4}'.format(i+1)
        trip = vt.Trip(trip_name, next(random_kms))
        trips.append(trip)
    return trips

def simulate_trip(trip, vehicle, vehicles_queue):
    trip_simulator = vt.VehicleTripSimulator(vehicle, trip, TRIP_UPDATE_INTERVAL)
    trip_simulator.run()
    vehicles_queue.enqueue(vehicle)

cosmos_db_logger = vt.VehicleLoggerCosmosDb(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY, COSMOS_DB_DATABASE_NAME, COSMOS_DB_CONTAINER_NAME)
cosmos_db_logger.connect()

vehicles_queue = create_vehicles_queue(TOTAL_VEHICLES, cosmos_db_logger)
trips = create_trips(TOTAL_TRIPS)

random_timer = iter(vt.RandomIntegerIterator(0, 120))
for trip in trips:
    while vehicles_queue.is_empty():
        pass
    vehicle = vehicles_queue.dequeue()
    timer = threading.Timer(next(random_timer), simulate_trip, [trip, vehicle, vehicles_queue])
    timer.start()


