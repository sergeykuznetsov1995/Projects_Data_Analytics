import pandahouse
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io

sns.set(
    font_scale=2,
    style="whitegrid",
    rc={'figure.figsize': (20, 7)}
)


def check_anomaly(df, metric, a=3, n=5):
    df['mean'] = df[metric].shift(1).rolling(n).mean()
    df['std'] = df[metric].shift(1).rolling(n).std()

    df['up'] = df['mean'] + a * df['std']
    df['low'] = df['mean'] - a * df['std']

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df


def run_alerts(chat=None):
    # сама система алертов
    chat_id = chat or -1001706798154
    bot = telegram.Bot(token='5226495929:AAHuW4J3UuMnG6mWPX-hAs4PCqMkagSvOnM')

    connection_project = dict(host='https://clickhouse.lab.karpov.courses',
                              user='student',
                              password='dpo_python_2020',
                              database='simulator_20220320')
    q = '''
        SELECT
            t1.ts as ts,
            t1.date as date,
            t1.hm as hm,
            users_feed,
            views,
            likes,
            CTR,
            users_message,
            sent_messages
        FROM (SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(time) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action='view') as views,
                countIf(user_id, action='like') as likes,
                likes/ views as CTR
            FROM simulator_20220320.feed_actions
            WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts) as t1
        join
        (select
            toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_message,
            count(*) as sent_messages
        from simulator_20220320.message_actions
        WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts) as t2
        using ts
        '''
    data = pandahouse.read_clickhouse(query=q, connection=connection_project)
    metrics_list = data.columns[3:].tolist()
    for metric in metrics_list:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_anomaly, df = check_anomaly(df, metric)

        if is_anomaly:
            msg = '''<a href = 'tg://user?id=219462759'>@sergeykuznetsov1995</a>\
            \nМетрика <u>{metric}</u>:\
            \nТекущее значение <b>{current_val:.2f}</b>\
            \nОтклонение от предыдущего значения <b>{last_val_diff:.2%}</b>\
            \n<a href='https://superset.lab.karpov.courses/superset/dashboard/639/'>See dashboard</a>'''.format(
                metric=metric,
                current_val=df[metric].iloc[-1],
                last_val_diff=abs(1 - (df[metric].iloc[-1] / df[metric].iloc[-2])))
            sns.set(rc={'figure.figsize': (16, 10)})  # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric)
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

            for ind, label in enumerate(
                    ax.get_xticklabels()):  # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time')  # задаем имя оси Х
            ax.set(ylabel=metric)  # задаем имя оси У\

            ax.set_title('{}'.format(metric))  # задае заголовок графика
            ax.set(ylim=(0, None))  # задаем лимит для оси У

            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=msg, parse_mode='HTML')


try:
    run_alerts()
except Exception as e:
    print(e)
