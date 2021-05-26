import os
import pandas as pd
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from IPython.display import HTML, display


def init_chrome():
    """Funcion para iniciar Chrome"""

    chrome_options = Options()

    useragent = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like "
        "Gecko) Chrome/73.0.3683.103 Safari/537.36"
    )

    # En windows C:\Users\<username>\AppData\Local\Google\Chrome\User Data\Default
    datadir = r"/home/eugenio/Global_Shapers/Bot_Comprar/OIA/TwitterBought/dags"
    chrome_options.add_argument(f"user-data-dir={datadir}") if datadir else None
    chrome_options.add_argument(f"user-agent={useragent}") if useragent else None
    chrome_options.add_argument("start-maximized")
    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("disable-blink-features=AutomationControlled")

    driver = Chrome(
        executable_path=os.path.join("chromedriver"
        ),  # esto puede ser chromedriver.exe si estas en windows
        options=chrome_options,
        desired_capabilities=chrome_options.to_capabilities(),
    )
    return driver


def extract_data(driver, th=0.5, verbose=True):
    """Para extraer tablas del HTML"""
    dfs = pd.read_html(driver.page_source)
    data = []
    for df in dfs:
        if df.isnull().sum().sum() / df.size > th:
            del df
            continue

        if verbose:
            display(HTML(df.to_html()))
        data.append(df.to_dict("list"))
    return data


def extract_panels(driver):
    """Para las tablas en el otro formato"""
    tables = driver.find_elements_by_xpath('//div[contains(@class, "panel")]')

    tablas = []
    for t in tables:
        try:
            title = t.find_element_by_xpath(".//h4").text
        except:
            title = None
        # iteramos tabla
        datos_tabla = []
        for r in t.find_elements_by_xpath('.//div[contains(@class, "row")]'):
            cols = r.find_elements_by_xpath('.//div[contains(@class, "col")]')
            row = {}
            for c in cols:
                try:
                    dato = c.find_element_by_xpath(".//span").text
                except:
                    dato = None
                    continue
                try:
                    label = c.find_element_by_xpath(".//label").text
                except:
                    label = title  # mm

                row[label] = dato

            try:
                del row[""]
            except:
                pass
            if len(row) > 0:
                datos_tabla.append(row)

        if len(datos_tabla) > 0:
            tablas.append(datos_tabla)
    return tablas
