from aiogram import Bot, Dispatcher
from tgbot.config_data.config import Config, load_config
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
import psycopg2
import logging
import smtplib

logger = logging.getLogger(__name__)

# В будущем следует создать отдельный модуль с запросами SQL
# Под некоторые запросы стоит выделить отдельные команды/кнопки
# и соответствующие хэндлеры для их обработки.
sql = ["""select request_datetime, device_id, battery, 
                temperature_battery_avg, errlist
        from public.monitor
        order by request_datetime desc
        limit 1000;""",

       """select * from public.akb
        order by request_datetime desc
        limit 1000;"""
]

config: Config = load_config()
dp = Dispatcher()

bot = Bot(
    token=config.tg_bot.token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

logger.info('Подключаюсь к базе данных...')
conn = psycopg2.connect(
    database=config.db.database,
    user=config.db.user,
    password=config.db.password,
    host=config.db.host)
logger.info('Успех!')


# Включать ли почтовую рассылку решаем при помощи 
# переменной окружения в .env

use_smtp = config.smtp.use_smtp

if use_smtp:
    recipients = [
        'i.r.kron@evm.eco',
        'technical.support@evm.eco'
    ]
else:
    recipients = None

def smtp_connect(is_spmt):
    if is_spmt:
        logger.info('Подключаюсь к SMTP-хосту...')
        smtp_host = smtplib.SMTP(
            host=config.smtp.host,
            port=config.smtp.port
        )
        logger.info('Авторизация...')
        smtp_host.login(config.smtp.login, config.smtp.password)
        logger.info('Успех!')
        return smtp_host
    return None

global smtp_host
smtp_host = smtp_connect(use_smtp)

