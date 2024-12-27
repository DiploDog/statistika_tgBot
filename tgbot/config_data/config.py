from dataclasses import dataclass
from environs import Env


@dataclass
class TgBot:
    token: str  # Токен для доступа к телеграм-боту


@dataclass
class Database:
    database: str
    user: str
    password: str
    host: str


@dataclass
class SMTPConn:
    host: str
    port: int
    login: str
    password: str
    use_smtp: bool


@dataclass
class Config:
    tg_bot: TgBot
    db: Database
    smtp: SMTPConn


def load_config(path: str | None = None) -> Config:
    env = Env()
    env.read_env(path)
    return Config(
        tg_bot=TgBot(
            token=env('BOT_TOKEN')
        ),
        db=Database(
            database=env('DB_NAME'),
            user=env('DB_USER'),
            password=env('PG_PASSWORD'),
            host=env('DB_HOST')
        ),
        smtp=SMTPConn(
            host=env('SMTP_HOST'),
            port=env('SMTP_PORT'),
            login=env('SMTP_LOGIN'),
            password=env('SMTP_PASSWORD'),
            use_smtp=env('USE_SMTP'),
            )
        )