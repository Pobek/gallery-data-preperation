import os
import csv
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

start_time = time.time()

load_dotenv()

DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")

db_string = f'postgres://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db = create_engine(db_string)
base = declarative_base()

class NetflixRecord(base):  
  __tablename__ = 'netflix_records'

  show_id = Column(Integer, primary_key=True)
  type = Column(String)
  title = Column(String)
  director = Column(ARRAY(String))
  cast = Column(ARRAY(String))
  country = Column(ARRAY(String))
  date_added = Column(String)
  release_year = Column(Integer)
  rating = Column(String)
  duration = Column(String)
  listed_in = Column(ARRAY(String))
  description = Column(String)

  def __repr__(self):
    ret = "NetflixRecord: \n"
    for key, value in self.__dict__.items():
      ret += f'{key} = {value},\n'
    return ret

def build_netflix_record(record):
  return NetflixRecord(
    show_id = record["show_id"],
    type = record["type"],
    title = record["title"],
    director = record["director"],
    cast = record["cast"],
    country = record["country"],
    date_added = record["date_added"],
    release_year = record["release_year"],
    rating = record["rating"],
    duration = record["duration"],
    listed_in = record["listed_in"],
    description = record["description"]
  )

def enrich_record(record):
  clone_record = record.copy()
  for key, value in clone_record.items():
    if key == "show_id" or key == "release_year":
      new_value = int(value)
      clone_record[key] = new_value
    elif "," in value and key != "date_added" and key != "title" and key != "description":
      new_value = value.split(", ")
      clone_record[key] = new_value
    elif key == "cast" or key == "country" or key == "listed_in" or key == "director":
      new_value = list()
      new_value.append(value)
      clone_record[key] = new_value
  return clone_record

def read_record(session):
  records = session.query(NetflixRecord)
  for record in records:
    print(f'{record.show_id} - {record.title}')


Session = sessionmaker(db)  
session = Session()

base.metadata.create_all(db)

with open("data/netflix_titles.csv", newline='') as csvfile:
  dictreader = csv.DictReader(csvfile)
  for row in dictreader:
    netflix_record = build_netflix_record(enrich_record(dict(row)))
    db_record = session.query(NetflixRecord).filter(NetflixRecord.show_id == netflix_record.show_id).first()
    if db_record:
      for key, value in netflix_record.__dict__.items():
        db_record.__dict__[key] = netflix_record.__dict__[key]
      session.commit()
    else:
      session.add(netflix_record)
      session.commit()

stop_time = time.time() - start_time
print(f'Done in {stop_time}')