import re
import datetime
from bs4 import BeautifulSoup
import requests
import asyncio
import asyncpg


async def connect_db(user, password, host, port, database):
    try:
        connection = await asyncpg.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        return connection
    except Exception as e:
        print("Ошибка с подключением в бд!")
        print(e)
        return None


async def parser_kg_ua(url):
    while True:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        body_page = soup.find(name="div", class_="ssilka_news1")

        link = body_page.find("a")
        href = link.get_attribute_list("href")
        link = url + href[0]
        news_page = requests.get(link)
        if news_page.status_code == 200:
            soup = BeautifulSoup(news_page.text, "html.parser")
            title = soup.find("h1").text
            body = soup.find("div", class_="field field-name-body field-type-text-with-summary field-label-hidden").text
            date = datetime.datetime.now()
            conn = await connect_db("postgres", "123456", "127.0.0.1", "5432", "NewsParser")
            if await conn.fetchrow(f"SELECT title FROM kg_news WHERE title = $1", title):
                print("data didn't add")
            else:
                if await conn.fetch(f"SELECT title FROM deleted_news WHERE title = '{title}' "):
                    print(f"[+]Post: {title}" + "was delete [+]")
                else:
                    await conn.execute(
                        "INSERT INTO kg_news (title, body, date, site) VALUES ($1, $2, $3, $4);",
                        title, body, date, "pravda_news"
                    )
                    print("data were add")
                    await conn.close()
        response = None
        soup = None
        body_page = None
        news = None
        item = None
        link = None
        news_page = None
        title = None
        info = None
        body = None
        href = None
        await asyncio.sleep(240)


async def parser_pravda_ua(url):
    while True:
        response = requests.get(url + "/news")
        soup = BeautifulSoup(response.text, "html.parser")
        body_page = soup.find(name="div", class_="article_header")

        link = body_page.find("a")
        href = link.get_attribute_list("href")[0]
        if "https" not in href:
            link = url + href
        else:
            link = href
        news_page = requests.get(link)
        if news_page.status_code == 200:
            soup = BeautifulSoup(news_page.text, "html.parser")
            title = soup.find("h1").text
            info = soup.find_all("p")
            body = ""
            date = datetime.datetime.now()
            for element in info:
                body += element.text + "\n"
            conn = await connect_db("postgres", "123456", "127.0.0.1", "5432", "NewsParser")
            if await conn.fetchrow(f"SELECT title FROM pravda_news WHERE title = '{title}' "):
                print("data didn't add")
            else:
                if await conn.fetch(f"SELECT title FROM deleted_news WHERE title = '{title}' "):
                    print(f"[+]Post: {title}" + "was delete [+]")
                else:
                    await conn.execute(
                        "INSERT INTO pravda_news (title, body, date, site) VALUES ($1, $2, $3, $4);",
                        title, body, date, "pravda_news"
                    )
                    print("data were add")
                await conn.close()
        response = None
        soup = None
        body_page = None
        news = None
        item = None
        link = None
        news_page = None
        title = None
        info = None
        body = None
        href = None
        await asyncio.sleep(240)


async def parser_rbc(url):
    url += "/"
    while True:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        body_page = soup.find(name="div", class_="container_sub_news")

        news = soup.find("div", class_="newsline")
        item = soup.find("div", class_="item")
        link = str(item.find("a").get_attribute_list("href")[0])
        news_page = requests.get(link)
        if news_page.status_code == 200:
            soup = BeautifulSoup(news_page.text, "html.parser")
            title = soup.find("article")
            title = title.find("h1").text.lstrip()
            info = soup.find_all("p")
            body = ""
            date = datetime.datetime.now()
            for element in info:
                body += element.text + "\n"
            conn = await connect_db("postgres", "123456", "127.0.0.1", "5432", "NewsParser")
            if await conn.fetchrow(f"SELECT title FROM rbc_news WHERE title = '{title}';"):
                print("data didn't add")
            else:
                if await conn.fetch(f"SELECT title FROM deleted_news WHERE title = '{title}' "):
                    print(f"[+]Post: {title}" + " was delete [+]")
                else:
                    await conn.execute(
                        "INSERT INTO rbc_news (title, body, date, site) VALUES ($1, $2, $3, $4);",
                        title, body, date, "rbc_news"
                    )
                    print("data were add")
                    await conn.close()
        response = None
        soup = None
        body_page = None
        news = None
        item = None
        link = None
        news_page = None
        title = None
        info = None
        body = None
        await asyncio.sleep(240)


def run():
    asyncio.run(parser())


async def parser():
    url_kg_ua = "https://kg.ua/"
    url_pravda_ua = "https://www.pravda.com.ua"
    url_rbc = "https://www.rbc.ua/rus/"

    await asyncio.gather(parser_kg_ua(url_kg_ua), parser_pravda_ua(url_pravda_ua), parser_rbc(url_rbc))


run()
