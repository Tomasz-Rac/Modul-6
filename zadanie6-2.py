import csv
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

DATABASE_URL = "sqlite:///weather_data.db"

engine = create_engine(DATABASE_URL, echo=True)

Base = declarative_base()

class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, unique=True, nullable=False)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)

class Measurement(Base):
    __tablename__ = "measurements"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String)
    date = Column(Date)
    precipitation = Column(Float)
    temperature = Column(Float)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def insert_stations(file_path):
    with open(file_path, mode="r", newline="") as file:
        reader = csv.reader(file)
        header = next(reader)
        print(f"CSV Header: {header}")

        for row in reader:
            try:
                print(f"Row Data: {row}")
                
                station = Station(
                    station_id=row[0].strip(),
                    name=row[1].strip(),
                    latitude=float(row[2].strip()),
                    longitude=float(row[3].strip()),
                    elevation=float(row[4].strip()) if row[4].strip() else None,
                )
                session.add(station)
            except ValueError as e:
                print(f"Skipping row due to error: {e}")

    session.commit()
    print("Stations data inserted successfully!")


def insert_measurements(file_path):
    with open(file_path, mode="r", newline="") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            measurement = Measurement(
                station_id=row[0],
                date=datetime.strptime(row[1], "%Y-%m-%d").date(),
                precipitation=float(row[2]) if row[2] else None,
                temperature=float(row[3]) if row[3] else None,
            )
            session.add(measurement)
    session.commit()
    print("Measurements data inserted successfully!")

insert_stations("clean_stations.csv")
insert_measurements("clean_measure.csv")

session.close()
print("Database setup complete!")
