from sqlmodel import SQLModel, create_engine, Session, Field, Column, Text
from dotenv import load_dotenv
import os 

load_dotenv(override=True)
db_user = os.getenv("DB_USER")
db_pwd = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

print(f"Probando conexión con: '{db_user}' y contraseña: '{db_pwd}'")
URL_CONNECTION = f"mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
engine = create_engine(URL_CONNECTION)

class Noticias(SQLModel, table=True):
    id: int | None= Field(default=None, primary_key=True)
    titulo: str
    url: str = Field(unique = True)
    cuerpo: str = Field(sa_column=Column(Text))

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)



