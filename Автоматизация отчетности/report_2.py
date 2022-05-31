import pandahouse
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
sns.set(
    font_scale =2,
    style      ="whitegrid",
    rc         ={'figure.figsize':(20,7)}
)

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
    q = ''' select 
                    t1.day as day,
                    t1.dau_feed as dau_feed,
                    t2.dau_message as dau_message,
                    t2.sended_messages as sended_messages,
                    t2.DIALOGS as DIALOGS
                from (select
                        toDate(time) as day,
                        count(distinct user_id) as dau_feed
                      from simulator_20220320.feed_actions
                      where toDate(time) between yesterday()-6 and yesterday()
                            AND
                            user_id NOT IN (SELECT user_id FROM simulator_20220320.message_actions )
                      group by day) as t1 
                INNER JOIN
                (select
                    toDate(time) as day,
                    count(distinct user_id) as dau_message,
                    count(*) as sended_messages,
                    uniqExact(user_id, reciever_id) as DIALOGS
                from simulator_20220320.message_actions
                where toDate(time) between yesterday()-6 and yesterday()
                group by day) as t2
                ON t1.day == t2.day '''
    feed_and_message = pandahouse.read_clickhouse(q, connection=connection)
    data_prepared = feed_and_message.set_index('day')
    fig, axes = plt.subplots(2, 2, figsize=(30, 20))
    title = f'Отчет за неделю {feed_and_message.day.dt.date.min()} - {feed_and_message.day.dt.date.max()}'
    fig.suptitle(title, size=32)
    axe = axes.ravel()
    for i, c in enumerate(data_prepared.columns):
        data_prepared[c].plot(ax=axe[i], title=data_prepared.columns[i], xlabel="")

    plot_object = io.BytesIO()
    fig.savefig(plot_object)
    plot_object.name = 'days7_graph.png'
    plot_object.seek(0)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object,  caption='REPORT #2')


try:
    test_report()
except Exception as e:
    print(e)
