import csv
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, user_id: int = 0) -> None:
        self.acc_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.user_id = user_id
        self.acc_file = None
        self.gps_file = None
        self.acc_reader = None
        self.gps_reader = None
        # Headers csv must have
        self._required_acc = {'x', 'y', 'z'}
        self._required_gps = {'longitude', 'latitude'}

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

    def read(self) -> AggregatedData:
        try:
            acc_row = next(self.acc_reader)
            gps_row = next(self.gps_reader)
        except StopIteration:
            # Infinite cycle logic 
            self.acc_file.seek(0)
            self.gps_file.seek(0)
            self.acc_reader = csv.DictReader(self.acc_file)
            self.gps_reader = csv.DictReader(self.gps_file)
            next(self.acc_reader) # Skip header row on re-read
            next(self.gps_reader)
            acc_row = next(self.acc_reader)
            gps_row = next(self.gps_reader)

        return AggregatedData(
            Accelerometer(int(acc_row['x']), int(acc_row['y']), int(acc_row['z'])),
            Gps(float(gps_row['longitude']), float(gps_row['latitude'])),
            datetime.now(),
            self.user_id
        )

    def stopReading(self):
        if self.acc_file: self.acc_file.close()
        if self.gps_file: self.gps_file.close()