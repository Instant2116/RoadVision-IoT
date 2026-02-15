import csv
from random import randint
from datetime import datetime
from domain.parking import Parking
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str, user_id: int) -> None:
        self.acc_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.user_id = user_id
        self.acc_file = None
        self.gps_file = None
        self.parking_file = None
        self.acc_reader = None
        self.gps_reader = None
        self.parking_reader = None
        # Headers csv must have
        self._required_acc = {'x', 'y', 'z'}
        self._required_gps = {'longitude', 'latitude'}
        self._required_parking = {'empty_count', 'longitude', 'latitude'}

    def _verify_headers(self, reader, required_fields, filename):
        if not reader.fieldnames:
            raise ValueError(f"File {filename} is empty or missing a header row.")
        
        missing = required_fields - set(reader.fieldnames)
        if missing:
            raise ValueError(f"File {filename} is missing required columns: {missing}")

    def startReading(self):
        print("started reading")
        self.acc_file = open(self.acc_filename, 'r')
        self.acc_reader = csv.DictReader(self.acc_file)
        self._verify_headers(self.acc_reader, self._required_acc, self.acc_filename)

        self.gps_file = open(self.gps_filename, 'r')
        self.gps_reader = csv.DictReader(self.gps_file)
        self._verify_headers(self.gps_reader, self._required_gps, self.gps_filename)

        self.parking_file = open(self.parking_filename, 'r')
        self.parking_reader = csv.DictReader(self.parking_file)
        self._verify_headers(self.parking_reader, self._required_parking, self.parking_filename)

    def __cycling_next(self, reader_attr_name, file):
        reader = getattr(self, reader_attr_name)
        try:
            res = next(reader)
        except StopIteration:
            file.seek(0)
            reader = csv.DictReader(file)
            next(reader) # Skip header row on re-read
            res = next(reader)
            setattr(self, reader_attr_name, reader)
        return res

    def _read_single_agg(self) -> AggregatedData:
        acc_row = self.__cycling_next('acc_reader', self.acc_file)
        gps_row = self.__cycling_next('gps_reader', self.gps_file)
        return AggregatedData(
            Accelerometer(int(acc_row['x']), int(acc_row['y']), int(acc_row['z'])),
            Gps(float(gps_row['longitude']), float(gps_row['latitude'])),
            datetime.now(),
            self.user_id
        )

    def _read_single_park(self) -> Parking:
        parking_row = self.__cycling_next('parking_reader', self.parking_file)
        return Parking(
            int(parking_row["empty_count"]),
            Gps(float(parking_row['longitude']), float(parking_row['latitude'])))

    def read(self) -> tuple[list[AggregatedData],list[Parking]]:
        return (
            [self._read_single_agg() for _ in range(randint(5, 7))],
            [self._read_single_park() for _ in range(randint(5, 7))]
            )

    def stopReading(self):
        if self.acc_file: self.acc_file.close()
        if self.gps_file: self.gps_file.close()
        if self.parking_file: self.parking_file.close()