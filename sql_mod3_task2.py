from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, MetaData, String, Float
import csv

engine = create_engine('sqlite:///database_csv.db')
meta = MetaData()


clean_measure = Table("clean_measure",
                        meta,
                        Column("id", Integer, primary_key=True),
                        Column("station", String),
                        Column("date", String),
                        Column("precip", Float),
                        Column("tobs", Integer),
                    )


clean_stations = Table("clean_stations",
                        meta,
                        Column("id", Integer, primary_key=True),
                        Column("station", String),
                        Column("latitude", Float),
                        Column("longitude", Float),
                        Column("elevation", Float),
                        Column("name", String),
                        Column("country", String),
                        Column("state", String)
                        )

meta.create_all(engine)

def open_csv_and_load_to_the_list(file_name): #without header
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        header= next(reader)
        data = []
        for row in reader:
            data.append(row)
        return data


def database_load_data_clean_measure(data):
    for i in data:
        ins = clean_measure.insert().values(station = i[0], 
                                            date = i[1], 
                                            precip = i[2], 
                                            tobs = i[3])
        conn = engine.connect()
        conn.execute(ins)


def database_load_data_clean_stations(data):
    for i in data:
        ins = clean_stations.insert().values(station = i[0], 
                                            latitude = i[1], 
                                            longitude = i[2], 
                                            elevation = i[3], 
                                            name = i[4], 
                                            country = i[5], 
                                            state = i[6])
        conn = engine.connect()
        conn.execute(ins)


if __name__ == "__main__":
    data_loaded_from_clean_measure = open_csv_and_load_to_the_list("clean_measure.csv")
    database_load_data_clean_measure(data_loaded_from_clean_measure)

    data_loaded_from_clean_stations = open_csv_and_load_to_the_list("clean_stations.csv")
    database_load_data_clean_stations(data_loaded_from_clean_stations)

    conn = engine.connect()
    show_5 = conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
    print(show_5)
        
   


    




