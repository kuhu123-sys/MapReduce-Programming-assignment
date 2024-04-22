import happybase
import pandas as pd

#Creating connection
connection = happybase.Connection('localhost', port = 9090, autoconnect=False)

#Opening connection
def open_connection():
    connection.open()

#Closing connection
def close_connection():
    connection.close()

#Getting the table
def fetch_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

#For inserting data
def insert_data(filename, tablename):
    print("Starting insert of the filename: ", filename)

    df = pd.read_csv(filename)
    df["TripID"] = range(1, len(df.index)+1)
    df.set_index("TripID", inplace=True)
    df.to_csv('filename1.csv', index=True)

    file = open('filename1.csv', 'r')
    table = fetch_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=20000) as a:
        for line in file:
            if i!=0:
                k = line.strip().split(",")
                a.put(k[0], {'cf1:VendorID': str(k[1]), 'cf1:tpep_pickup_datetime': str(k[2]), 'cf1:tpep_dropoff_datetime': str(k[3]), 'cf1:passenger_count': str(k[4]), 'cf1:trip_distance': str(k[5]), 'cf1:RatecodeID': str(k[6]), 'cf1:store_and_fwd_flag': str(k[7]), 'cf1:PULocationID': str(k[8]), 'cf1:DOLocationID': str(k[9]), 'cf1:payment_type': str(k[10]),'cf1:fare_amount': str(k[11]), 'cf1:extra': str(k[12]), 'cf1:mta_tax': str(k[13]), 'cf1:tip_amount': str(k[14]), 'cf1:tolls_amount': str(k[15]), 'cf1:improvement_surcharge': str(k[16]), 'cf1:total_amount': str(k[17]), 'cf1:congestion_surcharge': str(k[18]), 'cf1:airport_fee': str(k[19])})
            i = i+1
    file.close()
    print("Insertion completed for the filename: ", filename)
    close_connection()


insert_data('yellow_tripdata_2017-03.csv', 'TripData_hbase')
insert_data('yellow_tripdata_2017-04.csv', 'TripData_hbase')