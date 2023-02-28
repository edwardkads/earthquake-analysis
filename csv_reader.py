import csv

class CSVReader:
    def __init__(self, filename):
        self.filename = filename

    def load_csv_into_dict(self):
        # Let's load the contents of the csv file into a dictionary
        values = []
        with open(self.filename, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                values.append(dict(row))
        
        return values