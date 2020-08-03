import uuid
import time
import threading 
import numpy as np
from datetime import datetime, timezone

from azure.cosmos import exceptions, CosmosClient, PartitionKey

class VehicleLogger:
    def log(self, obj):
        raise NotImplementedError('You should implement this method.')

class Trip:
    def __init__(self, name, kms):
        self.id = uuid.uuid4().hex
        self.name = name
        self.total_kms = kms

class Vehicle:
    def __init__(self, plate_number, vehicle_logger):
        self.plate_number = plate_number
        self._vehicle_logger = vehicle_logger
        self.id = uuid.uuid4().hex
        self.status = 'stopped'
        self.current_trip = None
        self.current_kms = 0

    def assign_trip(self, trip):
        self.current_trip = trip

    def unasign_trip(self):
        self.current_trip = None
        self.current_kms = 0

    def _change_status_and_log(self, new_status):
        self.status = new_status
        self._vehicle_logger.log(self)
        if (self.current_trip is not None):
            print('Trip "{}" {} kms of {} kms : '.format(self.current_trip.name,self.current_kms,self.current_trip.total_kms),end='')
        print('Vehicle "{}" is in status "{}"'.format(self.plate_number,self.status))

    def log(self):
        self._vehicle_logger.log(self)

    def start_load(self):
        self._change_status_and_log('load-started')

    def finish_load(self):
        self._change_status_and_log('load-finished')

    def start_trip(self):
        self._change_status_and_log('trip-started')

    def continue_trip(self, kms):
        self.current_kms = kms
        self._change_status_and_log('trip-otw')

    def finish_trip(self, kms):
        self.current_kms = kms
        self._change_status_and_log('trip-finished')

    def start_unload(self):
        self._change_status_and_log('unload-started')

    def finish_unload(self):
        self._change_status_and_log('unload-finished')

    def stop(self):
        self._change_status_and_log('stopped')

class VehicleLoggerCosmosDb(VehicleLogger):
    def __init__(self, cosmos_db_endpoint, cosmos_db_key, cosmos_db_database_name, cosmos_db_container_name):
        self._cosmos_db_endpoint = cosmos_db_endpoint
        self._cosmos_db_key = cosmos_db_key
        self._cosmos_db_database_name = cosmos_db_database_name
        self._cosmos_db_container_name = cosmos_db_container_name
        self._simulation_timestamp = datetime.now()

    def connect(self):
        self._client = CosmosClient(self._cosmos_db_endpoint, self._cosmos_db_key)
        self._database = self._client.get_database_client(self._cosmos_db_database_name)        
        self._container = self._database.get_container_client(self._cosmos_db_container_name)
    
    def _format_timestamp(self, timestamp):
        return timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
    def log(self, vehicle):
        json = {
            'id': uuid.uuid4().hex,
            'plate_number': vehicle.plate_number,
            'vehicle_id': vehicle.id,
            'status': vehicle.status,
            'timestamp': self._format_timestamp(datetime.now()),
            'kms': float(vehicle.current_kms),
            'simulation_timestamp': self._format_timestamp(self._simulation_timestamp)
        } 
        if vehicle.current_trip is not None:
            json['trip_id'] = vehicle.current_trip.id
            json['trip_name'] = vehicle.current_trip.name
        self._container.create_item(body=json)        

class RandomIntegerIterator:        
    def __init__(self, low, high):
        self._low = low
        self._high = high

    def __iter__(self):
        self._rng = np.random.default_rng()
        return self       

    def __next__(self):
        return self._rng.integers(self._low,self._high)

class RandomVehiclePlateNumberIterator:
    def __init__(self):
        pass

    def __iter__(self):
        self._rng = np.random.default_rng()
        return self       

    def __next__(self):
        return '{} {}'.format(
            ''.join(self._rng.choice(list('0123456789'),4)),
            ''.join(self._rng.choice(list('ABCDFGHJKLMNOPRSTUVWXYZ'),3))
            )

class VehicleTripSimulator:
    def __init__(self, vehicle, trip, trip_update_interval):
        self.vehicle = vehicle
        self.trip = trip
        self._trip_update_interval = trip_update_interval
        self._current_kms = 0
        # Los valores de tiempo son en segundos, no en minutos, para hacer la simulación más rápida
        self._random_load_unload_time = iter(RandomIntegerIterator(40, 80))
        self._random_speed = iter(RandomIntegerIterator(int(80/60), int(130/60)))        

    def run(self):
        self.vehicle.assign_trip(self.trip)
        self.vehicle.start_load()
        time.sleep(next(self._random_load_unload_time))
        self.vehicle.finish_load()

        self.vehicle.start_trip()
        while self._current_kms < self.trip.total_kms:
            time.sleep(self._trip_update_interval)
            self._current_kms += self._trip_update_interval * next(self._random_speed)  
            self.vehicle.continue_trip(self._current_kms)
        self.vehicle.finish_trip(self.trip.total_kms)

        self.vehicle.start_unload()
        time.sleep(next(self._random_load_unload_time))
        self.vehicle.finish_unload()

        self.vehicle.unasign_trip()
        self.vehicle.stop()

class VehiclesQueue:
    def __init__(self):
        self._lock = threading.Lock()
        self._items = []

    def is_empty(self) :
        return self._items == []

    def enqueue(self, vehicle):
        self._lock.acquire()
        try:
            self._items.insert(0,vehicle)
        finally:
            self._lock.release()
            print('Vehicle "{}" was sent to the queue.'.format(vehicle.plate_number))

    def dequeue(self) :        
        self._lock.acquire()
        try:
            vehicle = self._items.pop()
        finally:
            self._lock.release()
            return vehicle
