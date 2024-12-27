import asyncio
import logging
import warnings
from typing import Iterable
import numpy as np
import pandas as pd
from datetime import timedelta
from aiogram import Router
from aiogram.filters import CommandStart
from tgbot.lexicon.lexicon import err_dict
from tgbot.loader import bot, conn, sql, smtp_connect, use_smtp, recipients, smtp_host
from email.mime.text import MIMEText
from smtplib import (SMTPSenderRefused, 
                     SMTPConnectError, 
                     SMTPServerDisconnected)
from smtplib import SMTP


warnings.filterwarnings(action='ignore', category=UserWarning)
logger = logging.getLogger(__name__)
user_router = Router()
notification_dict = dict()  # Словарь с отработавшими оповещениями

# ------------------------------------- Handlers ------------------------------------- #


@user_router.message(CommandStart())
async def command_start(message) -> None:
    """
    Хэндлер для обработки стартовой команды при запуске
    телеграм-бота, срабатывающий при нажатии на /start
    внутри самого бота.
    :param message: объект сообщения tg API
    :return:        None
    """
    logger.info('Начинаю опрос базы данных')
    counter = 1
    while True:
        logger.info(f'Requesting loop. Iteration № ' + str(counter))
        with conn.cursor() as curs:
            monitor = pd.read_sql_query(sql[0], conn)
            akb = pd.read_sql_query(sql[1], conn)

            # Условия фильтрации фреймов, при которых высылается уведомление
            battery_level_flag = monitor[monitor['battery'] < 5]
            error_flag = monitor[monitor['errlist'].isin(['P0A78', 'P0AFA', 'P0562'])]
            battery_temperature_flag = monitor[monitor['temperature_battery_avg'] > 35]

            # Обработка (декодирование и реструктурирование) датафрейма с
            # с данными аккумуляторной батареи (АКБ), а так же
            # его последуюшее аггрегирование с доп расчетами (см. док. на ф-ции)
            decoded_akb = await decode_raw_akb(akb)
            akb_flag = await calc_agg(decoded_akb)

            # Сообщения для уведомления при поступлении той или иной ошибки
            await error_alert(
                flag=battery_level_flag,
                flag_column='battery',
                err_log='Обнаружен низкий заряд у автомобиля',
                message=message,
                bot_send='Низкий заряд, %: '
            )
            await error_alert(
                flag=error_flag,
                flag_column='errlist',
                err_log='Обнаружена ошибка у автомобиля',
                message=message
            )
            await error_alert(
                flag=battery_temperature_flag,
                flag_column='temperature_battery_avg',
                err_log='Обнаружена высокая температура АКБ у автомобиля',
                message=message,
                bot_send='Высокая температура АКБ, град. С: '
            )
            await error_alert(
                flag=akb_flag,
                flag_column='message',
                err_log='Обнаружено низкое напряжение сегмента автомобиля',
                message=message,
                bot_send='segment'
            )
            await asyncio.sleep(10)
            counter += 1


# -------------------------------- Asynchronous functions -------------------------------- #


async def error_alert(flag: pd.DataFrame,
                      flag_column:  str,
                      err_log:      str,
                      message:      str,
                      bot_send:     str = 'err') -> None:
    """
    Функция фильтрации и формирования сообщения для
    телеграм-бота и почтовой рассылки по smtp
    
    :param flag:        кусок датафрейма, в котором была обнаружена ошибка
    :param flag_column: имя колонки, в которой была обнаружена ошибка
    :param err_log:     первичная часть сообщения; начало повествования
    :param message:     объект tg-API
    :param bot_send:    фильтр на следующую часть сообщения
    :return:            None
    """
    if flag.shape[0] != 0:
        # Выбираем строку с наиболее ранним упоминанием ошибки
        device_ids = []
        rows = flag.sort_values(by='request_datetime')[
            ['request_datetime', 'device_id', flag_column]
        ]
        for row in rows.iterrows():
            row = row[1]
            if (flag_column, row.device_id) not in device_ids:
                device_ids.append((flag_column, row.device_id))

                # На устранение ошибки дается два часа. При условии, если ошибка не устранена
                # и в базу данных приходит эта строка, то уведомление приходит снова с другим
                # временем обнаружения (+2 часа)
                if (flag_column, row.device_id) not in notification_dict or \
                        row.request_datetime > notification_dict[(flag_column, row.device_id)] \
                        + timedelta(hours=2):
                    logger.error(
                    f'{err_log} {row.device_id}:'
                    f' {row[flag_column]}; дата: {row.request_datetime}'
                    )
                    notification_dict[(flag_column, row.device_id)] = row.request_datetime
                    if bot_send == 'err':
                        string = f'Ошибка: {row.errlist}\nРасшифровка: {err_dict[row.errlist]}\n'
                    elif bot_send == 'segment':
                        string = 'Обнаружено низкое напряжение сегмента(-ов)'
                    else:
                        string = f'{bot_send}'
                    msg_text = f'ID: {str(row.device_id)}\n' + \
                        string + str(row[flag_column]) + '\n' +\
                        f'Дата и время обнаружения:\n' + \
                        f'{row.request_datetime}'
                    
                    await send_email(row, msg_text, err_log, recipients)

                    await bot.send_message(
                        message.chat.id,
                        msg_text
                    )
                    await bot.send_message(
                        chat_id='@EVMAlertChannel',
                        text=msg_text
                    )
        else:
            device_ids.clear()


async def decode_raw_akb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция, подготавливающая датафрейм с raw-data по АКБ
    
    :param df:  кусок фрейма из таблицы akb БД monitor
    :return:    декодированный фрейм
    """
    # Векторизация для применения к массиву перевода из HEX в INT
    vfunc = np.vectorize(lambda y: int(y, 16))  

    # Имеется 10 сегментов АКБ по 14 ячеек (12 физических + 2 виртуальные),
    # берем срез :280 (отбрасываем температуру в HEX), переводим в FLOAT.
    
    # Пример: имеем число b7, что в переводе в Decimal будет 183.
    # Для получения float значения напряжения ячейки, необходимо
    # вычислить его согласно формуле прямой 
    #
    #                   U_cell = kx + b,
    #
    # где     U_cell  - напряжение ячейки, В;
    #         k=0.01  - угол наклона прямой;
    #         b=1.5   - добавочный член (bias), В

    decode = lambda z: np.round(
        vfunc(
            np.array(
                [z[i:i+2] for i in range(0, len(z[:280]), 2)]
            )
        ) * 0.01 + 1.5,
        2)
    df['raw'] = df['raw'].apply(lambda x: decode(x))

    return df


async def calc_agg(df: pd.DataFrame, ll:float = 32.4) -> pd.DataFrame:
    """
    В функции к поданному на вход датафрейму применяются три расчетные
    функции, в частности для аггрегированного датафрейма
    :param df:  входной фрейм АКБ
    :param ll:  нижний предел ограничения величины напряжения сегмента АКБ
    :return:    готовый к фильтрации датафрейм с данными по АКБ
    """
    
    # Делим массив напряжений ячеек на 10 равных частей, к каждой
    # из которых применяем суммирование для вычисления напряжения сегмента
    df['aggregated'] = df['raw'].apply(
        lambda x: np.array(
            list(
                map(
                    mask_and_compress_sum,
                    np.array_split(
                        x,
                        10
                    )
                )
            )
        ).round(2)
    )

    # Определяем индексы, величины которых проходят условие < ll
    try:
        df['aggregated'] = df['aggregated'].apply(
            lambda x: {
                index: x[index]
                for index in list(np.where(x < ll)[0])
            }
        )
    except KeyError:
        df['aggregated'] = dict()
    
    # Формируем сообщение для оповещения
    df['message'] = df['aggregated'].apply(lambda x: from_akb_msg(x))
    df.dropna(inplace=True)
    return df


async def send_email(data:          pd.DataFrame,
                     msg_text:      str,
                     subj_text:     str,
                     destination:   list[str],
                     smtp_host:     SMTP=smtp_host) -> None:
    """
    Функция для отправки почтового сообщения с помощью smtp

    :param data:        фрейм данных
    :param msg_text:    текст почтового сообщения
    :param subj_text:   тема письма
    :param destination: получатели
    :return:            None
    """

    # Библиотека MIMEText используется как решения для smtp, т.к.
    # smtplib в sendmail и send_message понимает только ASCII
    mail_msg = MIMEText(msg_text, 'plain', 'utf-8')
    mail_msg['Subject'] = f'EVMAlert: {subj_text} {data.device_id}'
    mail_msg['From'] = 'i.r.kron@evm.eco'
    try:
        smtp_host.sendmail('i.r.kron@evm.eco', destination, mail_msg.as_string())
    except (SMTPSenderRefused, SMTPConnectError, SMTPServerDisconnected) as smtp_conn_err:
        smtp_host = smtp_connect(use_smtp)
        smtp_host.sendmail('i.r.kron@evm.eco', destination, mail_msg.as_string())


# ----------------------------------- Plain functions ----------------------------------- #


def mask_and_compress_sum(arr: Iterable[int|float], up_slice: int = 12) -> np.array:
    """
    Функция для отброса последних 2 виртуальных ячеек и подсчета суммы
    оставшихся 12-ти ячеек сегмента АКБ
    :param arr:      массив значений напряжений ячеек сегмента
    :param up_slice: число среза - с какого элемента удалить значения в массиве
    :return:         массив numpy со значением суммы напряжений ячеек (U сегмента)
    """
    arr = np.ma.array(arr, mask=False)
    arr.mask[up_slice:] = True
    arr = arr.compressed().sum()
    return arr


def from_akb_msg(x):
    """
    Функция для формирования текстового сообщения по АКБ
    :param x: словарь пар {№_сегмента: напряжение,_В}
    :return:  текст сообщения или None
    """
    msg = '\nСегмент:' + 3 * '\t' + 'Напряжение, В\n'
    try:
        items = x.items()
    except KeyError:
        logger.info('Ошибок не найдено, возвращаю None')
        return None
    if len(items) != 0:
        tabs = 17
        for k, v in items:
            if k == 9:
                tabs -= 2
            msg += f'{k+1}:' + tabs * '\t' + f'{v}\n'
        return msg
    return None
