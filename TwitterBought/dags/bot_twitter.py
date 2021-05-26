from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from consts import API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
from webscrapper import init_chrome, extract_data, extract_panels
import tweepy
import pandas as pd
from time import sleep
from IPython.display import HTML, display
import emoji

# Authenticate to Twitter
auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Create API object
api = tweepy.API(auth)


# TASKS


def _scrap_data():
    # SCRAP DATA FROM WEB COMPR.AR

    url = "https://comprar.gob.ar/"
    pausa = 3

    driver = init_chrome()
    driver.get(url)
    sleep(pausa)

    # xpath cheatsheet https://devhints.io/xpath
    driver.find_element_by_xpath('//*[contains(text(), "Ver todos")]').click()

    # primera tabla con las
    df = pd.read_html(driver.page_source)[0]
    df = df.drop([c for c in df.columns if "Unnamed" in c], axis=1).iloc[:-2, :]
    display(HTML(df.to_html()))
    print("--" * 50)

    data = []
    # para cada licitacion
    for n in range(10):
        data_licitacion = []
        # seleccionamos licitacion
        elem = driver.find_elements_by_xpath("//tr/td[1]/a")[n]
        # inicializamos "base" con el id
        licitacion_id = elem.text
        elem.click()
        sleep(pausa)

        # vemos solo las tablas de la primera
        data_licitacion.extend(extract_data(driver=driver, verbose=True if n == 0 else False))
        data_licitacion.extend(extract_panels(driver=driver))

        # dice monto?
        if "Ofertas al proceso de compra" in driver.page_source:
            driver.find_element_by_xpath(
                "//*[contains(text(), 'cuadro comparativo')]"
            ).click()
            data_licitacion.extend(extract_data(verbose=True if n == 0 else False))
            data_licitacion.extend(extract_panels())
            driver.back()

        posibles_tablas = [
            e.text for e in driver.find_elements_by_xpath("//h4") if len(e.text) > 0
        ]
        data.append(data_licitacion)

        driver.back()
        sleep(pausa)
        print("--" * 50)
        break

    # RETURN THE DATA OBJECT
    return data


def _format_data(ti):
    # FETCH RAW DATA FROM WEB SCRAPPING TASK
    data = ti.xcom_pull(task_ids="scrap_data")

    # PRE PROCESS DATA
    df_info_proceso = pd.DataFrame.from_dict(data=data[0][12][0], orient="index")
    df_moneda_y_alcance = pd.DataFrame.from_dict(data[0][12][2], orient="index")
    df_detalle_productos = pd.DataFrame(data[0][1])

    productos = []
    cantidad = []
    for item in range(len(df_detalle_productos)):

        desc = df_detalle_productos.iloc[item]["Descripción"]
        productos.append(desc)

        q = df_detalle_productos.iloc[item]["Cantidad"]
        q = q.split(" ")[0]
        cantidad.append(q)

    data_processed = {
        "alcance": df_moneda_y_alcance[0][0],
        "moneda": df_moneda_y_alcance[0][1],
        "titulo": df_info_proceso[0][1],
        "productos": list(set(productos)),
        "cantidad": list(set(cantidad))
    }

    # RETURN DATA FORMATTED
    return data_processed


def _publish_tweet(ti):

    # FETCH FORMATTED DATA FROM FORMAT DATA TASK
    data_processed = ti.xcom_pull(task_ids="format_data")

    # CREATE AND PUBLISH TWEET OBJECT WITH IT
    for item in range(len(data_processed["productos"])):

        tweet = emoji.emojize(
            f"""
        \033[1m:package:Titulo:\033[0m {data_processed['titulo']}\n
        \033[1m:dollar:Moneda:\033[0m {data_processed['moneda']}\n
        \033[1m:earth_americas:Alcance:\033[0m {data_processed['alcance']}\n
        \033[1m:hospital:Producto:\033[0m {data_processed['productos'][item]}\n
        \033[1m:white_check_mark:Cantidad:\033[0m {data_processed['cantidad'][item]}
        """,
            use_aliases=True,
        )

        # TWEET
        print(f'{item}',tweet)
        api.update_status(tweet)


# first time triggered at start_date + schedule_interval
with DAG(
    "bot_twitter",
    start_date=datetime(2021, 5, 23),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    scrap_data = PythonOperator(task_id="scrap_data", python_callable=_scrap_data)

    format_data = PythonOperator(task_id="format_data", python_callable=_format_data)

    publish_tweet = PythonOperator(
        task_id="publish_tweet", python_callable=_publish_tweet
    )

scrap_data >> format_data >> publish_tweet
