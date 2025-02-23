import json
import pandas as pd
from pydoop import hdfs

# HDFS path to your JSON files and destination for CSV output
hdfs_json_dir = '/training/flume/stock'
hdfs_csv_output = '/training/flume/stock_csv/'

# Local temporary path to store CSV files
local_csv_path = '/tmp/converted_stock_data.csv'

# Function to process JSON data from HDFS
def process_json_files_from_hdfs():
    # List of dictionaries to hold each row of the flattened data
    rows = []
    
    # List files in the HDFS JSON directory
    json_files = hdfs.ls(hdfs_json_dir)

    for json_file in json_files:
        # Open each JSON file
        with hdfs.open(json_file, 'rt') as f:
            for line in f:
                # Load the JSON line as a dictionary
                data = json.loads(line)
                for stock, details in data.items():
                    for datetime, attributes in details.items():
                        # Add stock and datetime info to the attributes
                        row = {
                            'stock': stock,
                            'datetime': datetime,
                            **attributes
                        }
                        rows.append(row)

    # Create a DataFrame from rows
    df = pd.DataFrame(rows)
    df = df[['stock', 'datetime', 'volume', 'open', 'high', 'low', 'close']]  # Optional: specify column order

    # Write to local CSV file
    df.to_csv(local_csv_path, index=False)

    # Upload CSV back to HDFS
    hdfs.put(local_csv_path, hdfs_csv_output + 'stock_data.csv')

# Run the function to process files and create CSV in HDFS
process_json_files_from_hdfs()
