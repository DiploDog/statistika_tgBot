FROM python:3.11.9
ENV BOT_NAME=statistika_bot

WORKDIR /usr/src/app/${BOT_NAME:-tg_bot}

COPY requirements.txt /usr/src/app/${BOT_NAME:-tg_bot}
RUN python -m pip install --upgrade pip
RUN python -m pip install -r /usr/src/app/${BOT_NAME:-tg_bot}/requirements.txt
COPY . /usr/src/app/${BOT_NAME:-tg_bot}
