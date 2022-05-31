import pandahouse
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io

sns.set_style("darkgrid", {"grid.color": ".6", "grid.linestyle": ":"})


def test_report(chat=None):
    chat_id = chat or -1001539201117
    bot = telegram.Bot(token='5226495929:AAHuW4J3UuMnG6mWPX-hAs4PCqMkagSvOnM')
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220320'
    }
    q = '''SELECT * FROM 
    (SELECT toDate(time) AS day,
          countIf(action='view') AS views,
          countIf(action='like') AS likes,
          round(countIf(action='like') / countIf(action='view'), 2) AS "CTR",
          count(DISTINCT user_id) AS DAU
    FROM simulator_20220320.feed_actions 
    WHERE toDate(time) between today() - 7 and today() - 1
    GROUP BY toDate(time)) f 
    '''
    feed = pandahouse.read_clickhouse(q, connection=connection)
    DAU = feed.DAU.loc[feed.day.idxmax()]
    views = feed.views.loc[feed.day.idxmax()]
    likes = feed.likes.loc[feed.day.idxmax()]
    CTR = feed.CTR.loc[feed.day.idxmax()]
    msg = f'REPORT №1 \nКлючевые метрики за вчерашний день {feed.day.dt.date.max()}: \nDAU: {DAU} \nПросмотры: {views} \nЛайки: {likes} \nCTR: {CTR}'
    data_prepared = feed.set_index('day')
    fig, axes = plt.subplots(2, 2, figsize=(30, 20))
    title = f'Отчет за неделю {feed.day.dt.date.min()} - {feed.day.dt.date.max()}'
    fig.suptitle(title, size=32)
    axe = axes.ravel()
    for i, c in enumerate(data_prepared.columns):
        data_prepared[c].plot(ax=axe[i], title=data_prepared.columns[i], xlabel="")
    plot_object = io.BytesIO()
    fig.savefig(plot_object)
    plot_object.name = 'days7_graph.png'
    plot_object.seek(0)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=msg)


try:
    test_report()
except Exception as e:
    print(e)
