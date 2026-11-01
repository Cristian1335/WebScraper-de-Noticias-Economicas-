#Requests permite enviar peticiones HTTP de manera sencilla, 
# mientras que BeautifulSoup permite parsear y extraer información de documentos HTML y XML.
from celery import Celery
import requests, time, random
from bs4 import BeautifulSoup
from sqlmodel import Session,select
from urllib.parse import urljoin
from models import URL_CONNECTION, engine, Noticias, create_db_and_tables
from celery.schedules import crontab

app = Celery(
    "tasks",
    backend=f"db+{URL_CONNECTION}",
    broker='pyamqp://guest@localhost//'
)

# La configuración para el reloj automático
app.conf.beat_schedule = {
    "extraer-minuto-ambito":{
       "task": "celery_app.extraer_ambito",
        "schedule": crontab(minute="*/14") 
    },
    "extraer-tn":{
        "task":"celery_app.extraer_tn",
        "schedule":crontab(minute="*/12") # */ es lo que indica que se repita cada x tiempo, en este caso, 12 mins.
    },
    "extraer-infobae":{
        "task":"celery_app.extraer_noticias_infobae",
        "schedule":crontab(minute="*/13")
    }
}

@app.task
def extraer_noticias_infobae():
    create_db_and_tables()
    url_base = "https://www.infobae.com/economia/"
    response = requests.get(url_base)

    if response.status_code == 200:
    # 2. Pasar el texto crudo por el "colador"
        soup = BeautifulSoup(response.text, 'html.parser')
    # elimino todo, dejo sólo la informacion que quiero
        titulos = soup.find_all("h2", class_="story-card-hl" )

        for titulo in titulos[:10]: 
            # 1. Extraemos el texto limpio como antes
            texto_limpio= titulo.text.strip()
            # 2. Buscamos la etiqueta <a> que envuelve a este <h2>
            etiqueta_a = titulo.find_parent("a")

            # 3. Validamos que exista y extraemos el valor del atributo 'href'
            if etiqueta_a:

                #4 Ahora voy a hacer que el scraper ingrese en cada noticia para extraer la informacion
                #4.1 armo el enlace absoluto para navegar a la noticia
                ruta_relativa = str(etiqueta_a["href"])
                url_completa = urljoin(url_base, ruta_relativa)
                pet_noticia = requests.get(url_completa)

                if pet_noticia.status_code == 200:
                    soup_noticias = BeautifulSoup(pet_noticia.text, "html.parser") #Nuevo arbol exclusivo para la noticia
                    parrafos = soup_noticias.find_all("p") #busco todos los parrafos de la noticia
                    # Unimos el texto de todos los párrafos ignorando los vacíos
                    cuerpo = " ".join([p.text.strip() for p in parrafos if p.text.strip()])

                    with Session(engine) as db:
                        statement = select(Noticias).where(Noticias.url == url_completa)
                        noticia_existente = db.exec(statement).first()
                        if not noticia_existente:
                            nueva_noticia = Noticias(
                                titulo = texto_limpio,
                                url = url_completa,
                                cuerpo = cuerpo
                            )
                            db.add(nueva_noticia)
                            db.commit()
                            tiempo_espera = random.uniform(2.3,4.5)
                            time.sleep(tiempo_espera) # tiempo de espera para simular comportamiento humano
                        else:
                            print(f"se detectó noticia repetida: {texto_limpio}")
        return "Infobae scrapping finalizado exitosamente"
    else: 
        print("Error, no pudo descargarse el contenido")

@app.task
def extraer_tn():
    url_base="https://tn.com.ar/economia/"    
    response = requests.get(url_base)    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        tarjetas = soup.find_all("article", class_="card__container")
        for tarjeta in tarjetas[:10]:
            link = tarjeta.find("a", class_="card__image")

            if link and "href" in link.attrs:
                titulo = link.get("title","").strip() #type:ignore
                ruta_relativa = str(link["href"])
                url_completa = urljoin(url_base, ruta_relativa)
                pet_noticia = requests.get(url_completa)

                if pet_noticia.status_code == 200:
                    soup_noticias = BeautifulSoup(pet_noticia.text, "html.parser")
                    cuerpo_div = soup_noticias.find("div", class_="article__body")
                    if cuerpo_div:
                        parrafos = cuerpo_div.find_all("p",class_="paragraph")#type:ignore
                        cuerpo = " ".join([p.text.strip() for p in parrafos if p.text.strip()])
                    
                        with Session(engine) as db:
                            statement = select(Noticias).where(Noticias.url == url_completa)
                            noticia_existente = db.exec(statement).first()
                            if not noticia_existente:
                                nueva_noticia = Noticias(titulo=titulo, url=url_completa, cuerpo=cuerpo)
                                db.add(nueva_noticia)
                                db.commit()
                                tiempo_espera = random.uniform(2.3,4.5)
                                time.sleep(tiempo_espera) # tiempo de espera para simular comportamiento humano
                            else:
                                print(f"se detecto noticia repetida: {titulo}")
        return "TN scrapeado con éxito"
    else:
        return "Falló conexión a TN"

#Este encabezado fue necesario para que no me patearan el script por bot
encabezados = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
@app.task
def extraer_ambito():
    url_base = "https://www.ambito.com/finanzas"
    response = requests.get(url_base, headers=encabezados)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        news = soup.find_all("article", class_="news-article")
        for new in news[:10]:
            title_h2 = new.find("h2",class_="news-article__title")
            if title_h2: 
                link = title_h2.find("a")
                if link and "href" in link.attrs:
                    titulo = link.get("title", "").strip()#type:ignore
                    url_completa= str(link["href"])

                    pet_noticia = requests.get(url_completa,headers=encabezados)
                    if pet_noticia.status_code == 200:
                        soup2 = BeautifulSoup(pet_noticia.text,"html.parser")
                        noticia_cuerpo = soup2.find("article", class_="article-body")
                        if noticia_cuerpo:
                            parrafos = noticia_cuerpo.find_all("p")
                            contenido = " ".join([p.text.strip() for p in parrafos if p.text.strip()])
                            with Session(engine) as db:
                                statement = select(Noticias).where(Noticias.url == url_completa)
                                noticia_existente = db.exec(statement).first()
                                if not noticia_existente: 
                                    nueva_noticia = Noticias(
                                    titulo = titulo,
                                    url = url_completa,
                                    cuerpo = contenido
                                    )
                                    db.add(nueva_noticia)
                                    db.commit()
                                    tiempo_espera = random.uniform(2.3,4.5)
                                    time.sleep(tiempo_espera) # tiempo de espera para simular comportamiento humano
                                else:
                                    print(f"se deteco noticia repetida: {titulo}")
        return "El ambito scrappeado exitosamente"
    else:
        return "No pudo conectarse con El Ambito"



