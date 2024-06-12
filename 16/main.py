import pandas as pd
import wikipedia as wp
import sqlite3
from pypika import Query, Table, Case, functions as fn


html = wp.page("List_of_Doctor_Who_episodes_(2005–present)").html().encode("UTF-8")

try:
    df = pd.read_html(html)[1]
except IndexError:
    df = pd.read_html(html)[0]

df = df.iloc[:300, :]

episodes_df = df.iloc[:, [0, 1, 2, 3]].rename(columns=lambda x: x.replace(']', '').replace('[', '').strip())
episodes_df.columns = ['title', 'episode_number', 'air_date', 'viewers']
conn = sqlite3.connect("doctor_who_episodes.db")
episodes_df.to_sql("episodes", conn, if_exists="replace", index=False)
conn.close()

print("Таблица создана успешно!")
conn = sqlite3.connect("doctor_who_episodes.db")
cursor = conn.cursor()

episodes = Table("episodes")

#Запрос для получения информации о названии эпизода, номере, дате выхода и количестве просмотров
query = (
    Query.from_(episodes)
    .select(episodes.title, episodes.episode_number, episodes.air_date, episodes.viewers)
)
cursor.execute(str(query))
print("Информация о эпизодах Доктора Кто:")
for row in cursor.fetchall():
    print(row)

#Запрос с группировкой по годам и подсчетом количества эпизодов, вышедших в каждом году
query = (
    Query.from_(episodes)
    .select(fn.Extract('year', episodes.air_date).as_("year"), fn.Count('*').as_("episode_count"))
    .groupby(fn.Extract('year', episodes.air_date))
)


conn.close()
