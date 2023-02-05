#!/usr/bin/python3
from confluent_kafka import Producer
from flask import Flask, request
from werkzeug.exceptions import HTTPException
from time import strftime
import json
import logging

# Определяем переменные конфигурации
log_file_name = 'log.txt'
logging_in_file = True  # True - пишет в файл, False - на консоль
format_datetime = '%d.%m.%Y %H:%M:%S'
format_logging = "%(asctime)s: |%(name)s %(levelname)s| %(message)s'"
kafka_broker = '192.168.175.139:9092'
# kafka_topic = 'user-tracker'
kafka_topic = 'main-rest'

# Инициализация
app = Flask(__name__)
p = Producer({'bootstrap.servers': kafka_broker})

# Тестовый json, отдаваемый при GET
data = {"Title": "It is test kafka APP", "Description": "To put message in kafka send JSON over POST-request"}

# Определяем логирование
if logging_in_file:
    logging.basicConfig(filename=log_file_name, filemode='a', format=format_logging, datefmt=format_datetime,
                        level=logging.INFO)
    logging.info("Log started")
else:
    logging.basicConfig(level=logging.INFO)


# Callback функция для продюсера
def receipt(err, msg):
    if err is not None:
        logging.error('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logging.info(message)


# Запись в кафку (принимает на вход словарь / json)
def kafka_send_message(input_data):
    m = json.dumps(input_data)
    p.poll(1)
    p.produce(kafka_topic, m.encode('utf-8'), callback=receipt)
    p.flush()
    print(f"Message send to kafka:\n{input_data}")


# Прописываем роуты
# GET для корневой страницы: выдает JSON с data
@app.route('/', methods=['GET'])
def test_get_request():
    return data


# POST для корневой страницы: принимает JSON, кладет его в кафку
@app.route('/', methods=['POST'])
def test_post_request():
    dict_json = request.json
    dict_json.update({"date_insert": strftime(format_datetime)})  # Добавляем ключ-значение с датой

    kafka_send_message(dict_json)
    return dict_json


# Обработка ошибок: в случае ошибки будет выведен JSON с основной информацией
@app.errorhandler(HTTPException)
def handle_exception(e):
    response = e.get_response()
    response.data = json.dumps({
        "date_insert": strftime(format_datetime),
        "code": e.code,
        "name": e.name,
        "description": e.description
    })
    response.content_type = "application/json"

    kafka_send_message(response.json)
    return response


# Запуск приложения
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
