from csv_reader import CSVReader
from collections import Counter, defaultdict
from constants import *
from datetime import datetime
import dateutil
import pytz


class EarthquakeCalculator:
    def __init__(self, filename: str):
        # load csv data into a list
        self.values = CSVReader(filename).load_csv_into_dict()

    def get_location_source_with_most_earthquakes(self) -> tuple:
        counter = Counter()
        for row in self.values:
            counter[row[LOCATION_SOURCE_KEY]] += 1
        location_source_with_most_earthquakes = counter.most_common(1)[0]
        return location_source_with_most_earthquakes

    def get_earthquakes_per_day_histogram_data(self, timezone: str) -> list:
        #histogram_data = [["date", "earthquakes per day"]]
        histogram_data = defaultdict(int)
        # if invalid timezone use UTC
        try:
            timezone = pytz.timezone(timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            timezone = pytz.timezone('UTC')

        for row in self.values:
            date = datetime.strptime(row[TIMESTAMP_KEY], TIMESTAMP_FORMAT)
            timezone_date = date.astimezone(timezone)
            datestring = timezone_date.strftime('%Y/%m/%d')
            histogram_data[datestring] += 1
     
        return [(k, v) for k,v in histogram_data.items()]

    def get_average_earthquake_magnitude_per_location_source(self) -> dict:
        data = dict()
        for row in self.values:
            if row[LOCATION_SOURCE_KEY] in data:
                data[row[LOCATION_SOURCE_KEY]][0] += float(row[MAGNITUDE_KEY])
                data[row[LOCATION_SOURCE_KEY]][1] += 1
            else:
                data[row[LOCATION_SOURCE_KEY]] = [float(row[MAGNITUDE_KEY]),1]
        average_earthquake_per_location = dict()
        for k,v in data.items():
            average_earthquake_per_location[k] = round(v[0]/v[1],2)
        return average_earthquake_per_location


class RealTimeEarthquakeStreamData:
    def __init__(self):
        self.location_source_dict = dict()

    def process_stream(self, stream_data: dict) -> None:
        """
        Processes stream of earthquake data
        example stream
        {'time': '2023-02-26T01:08:01.550Z', 'latitude': '35.7176666', 'longitude': '-121.0634995', 'depth': '5.52', 'mag': '1.92',
         'magType': 'md', 'nst': '18', 'gap': '90', 'dmin': '0.08144', 'rms': '0.04', 'net': 'nc', 
         'id': 'nc73851811', 'updated': '2023-02-26T01:21:11.944Z', 
         'place': '14km NE of San Simeon, CA', 'type': 'earthquake', 'horizontalError': '0.43', 
         'depthError': '0.89', 'magError': '0.12', 'magNst': '15', 'status': 'automatic', 'locationSource': 'nc', 
         'magSource': 'nc'
        }
        """
        if stream_data[LOCATION_SOURCE_KEY] in self.location_source_dict:
            self.location_source_dict[stream_data[LOCATION_SOURCE_KEY]][0] += float(stream_data[MAGNITUDE_KEY])
            self.location_source_dict[stream_data[LOCATION_SOURCE_KEY]][1] += 1
        else:
            self.location_source_dict[stream_data[LOCATION_SOURCE_KEY]] = [float(stream_data[MAGNITUDE_KEY]), 1]
        
    
    def get_average_earthquake_magnitude_per_location_source(self) -> dict:
        average_earthquake_per_location = dict()
        for location_source, magnitude_data in self.location_source_dict.items():
            average_earthquake_per_location[location_source] = round(magnitude_data[0]/magnitude_data[1], 2)
        return average_earthquake_per_location



if __name__ == '__main__':
    # Run the solution and print the results
    earthquake_compute = EarthquakeCalculator('1.0_month.csv')
    print("=" * 150)
    print("Location source with most earthquake is ", earthquake_compute.get_location_source_with_most_earthquakes())
    print("=" * 150)
    print("Earthquakes per day histogram data in UTC = \n", earthquake_compute.get_earthquakes_per_day_histogram_data('UTC'))
    print("=" * 150)
    print("Earthquakes per day histogram data in PST = \n", earthquake_compute.get_earthquakes_per_day_histogram_data('US/Pacific'))
    print("=" * 150)
    print("Average earthquake magnitude per location source is, ", earthquake_compute.get_average_earthquake_magnitude_per_location_source())
    print("=" * 150)
    print("Real time earthquake stream data")
    realtimestream = RealTimeEarthquakeStreamData()
    print("Processing stream")
    realtimestream.process_stream({'time': '2023-02-26T01:08:01.550Z', 'latitude': '35.7176666', 
    'longitude': '-121.0634995', 'depth': '5.52', 'mag': '1.92', 'magType': 'md', 'nst': '18',
     'gap': '90', 'dmin': '0.08144', 'rms': '0.04', 'net': 'nc', 'id': 'nc73851811',
     'updated': '2023-02-26T01:21:11.944Z', 'place': '14km NE of San Simeon, CA', 
     'type': 'earthquake', 'horizontalError': '0.43', 'depthError': '0.89', 
      'magError': '0.12', 'magNst': '15', 'status': 'automatic', 
      'locationSource': 'nc', 'magSource': 'nc'})
    print("average earthquake data is ", realtimestream.get_average_earthquake_magnitude_per_location_source())
    print("Processing stream")
    realtimestream.process_stream({'time': '2023-02-26T01:08:01.550Z', 'latitude': '35.7176666', 
    'longitude': '-121.0634995', 'depth': '5.52', 'mag': '1.70', 'magType': 'md', 'nst': '18',
     'gap': '90', 'dmin': '0.08144', 'rms': '0.04', 'net': 'nc', 'id': 'nc73851811',
     'updated': '2023-02-26T01:21:11.944Z', 'place': '14km NE of San Simeon, CA', 
     'type': 'earthquake', 'horizontalError': '0.43', 'depthError': '0.89', 
      'magError': '0.12', 'magNst': '15', 'status': 'automatic', 
      'locationSource': 'nc', 'magSource': 'nc'})
    print("average earthquake data is ", realtimestream.get_average_earthquake_magnitude_per_location_source())
    print("Processing stream")
    realtimestream.process_stream({'time': '2023-02-26T00:31:02.080Z', 'latitude': '17.912', 'longitude': '-66.969', 
        'depth': '11', 'mag': '3.45', 'magType': 'md', 'nst': '19', 'gap': '213', 'dmin': '0.0946', 
        'rms': '0.12', 'net': 'pr', 'id': 'pr2023057002', 'updated': '2023-02-26T01:03:21.242Z', 
        'place': 'Puerto Rico region', 'type': 'earthquake', 'horizontalError': '0.5', 'depthError': '0.35', 
        'magError': '0.19', 'magNst': '13', 'status': 'reviewed', 'locationSource': 'pr', 'magSource': 'pr'})
    print("average earthquake data is ", realtimestream.get_average_earthquake_magnitude_per_location_source())
    print("Processing stream")
    realtimestream.process_stream({'time': '2023-02-25T23:20:04.825Z', 'latitude': '64.8791', 'longitude': '-147.0753', 
        'depth': '18.2', 'mag': '1.1', 'magType': 'ml', 'nst': '', 'gap': '', 'dmin': '',
        'rms': '0.64', 'net': 'ak', 'id': 'ak0232kzwpbl', 'updated': '2023-02-25T23:22:57.508Z',
        'place': '1 km WNW of Two Rivers, Alaska', 'type': 'earthquake', 'horizontalError': '', 
        'depthError': '0.3', 'magError': '', 'magNst': '', 'status': 'automatic', 'locationSource': 'ak', 'magSource': 'ak'})
    print("average earthquake data is ", realtimestream.get_average_earthquake_magnitude_per_location_source())