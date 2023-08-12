from flask import Flask, render_template, url_for, redirect
import psycopg2

app = Flask(__name__)


def connect_db(user, password, host, port, database):
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        return connection
    except:
        print("Ошибка с подключением в бд!")
        return None


@app.route("/")
def index():
    conn = connect_db("postgres", "123456", "127.0.0.1", "5432", "NewsParser")
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM kg_news UNION SELECT * FROM pravda_news UNION SELECT * FROM rbc_news ORDER BY date DESC;")
        news = cur.fetchall()
    except:
        print("Something wrong")
    return render_template("index.html", news=news)


@app.route("/delete_post/<int:id>/<site>/<title>")
def delete_post(id, site, title):
    conn = connect_db("postgres", "123456", "127.0.0.1", "5432", "NewsParser")
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {site} WHERE id='{id}';")
        cur.execute(f"INSERT INTO deleted_news (title) VALUES('{title}')")
    except Exception as e:
        print(e)
    finally:
        conn.commit()
        conn.close()
        return redirect(url_for("index"))


if __name__ == "__main__":
    app.run()
