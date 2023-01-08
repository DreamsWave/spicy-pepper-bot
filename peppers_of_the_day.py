import logging
import telebot
import os
import ydb
import ydb.iam
from datetime import datetime
from dotenv import load_dotenv
import openai

# init
load_dotenv()
logger = telebot.logger
telebot.logger.setLevel(logging.INFO)
bot = telebot.TeleBot(os.getenv('TELEGRAM_TOKEN'), threaded=False, parse_mode="HTML")
driver = ydb.Driver(
        endpoint = os.getenv("YDB_ENDPOINT"), 
        database = os.getenv("YDB_DATABASE"),
        credentials = ydb.iam.ServiceAccountCredentials.from_file(os.getenv("SA_KEY_FILE")) if os.getenv("LAMBDA_RUNTIME_DIR") is None else None
    )
driver.wait(timeout=15)
pool = ydb.SessionPool(driver)

openai.api_key = os.getenv("OPENAI_API_KEY")

### Main handler
def handler(event, context):
    peppers_of_the_day = get_peppers_of_the_day()
    if peppers_of_the_day:
        random_fact = get_random_fact()
        for pepper_of_the_day in peppers_of_the_day:
            random_pepper = get_random_pepper(pepper_of_the_day.chat_id)
            update_pepper_of_the_day(random_pepper.chat_id, random_pepper.user_id)

            bot.send_message(
                chat_id=pepper_of_the_day.chat_id, 
                text="<b>@{0}</b>, поздравляю! У тебя сегодня самый лучший перчик!".format(random_pepper.username), 
                parse_mode="HTML", 
                disable_notification=True
            )
            
            bot.send_message(
                chat_id=pepper_of_the_day.chat_id, 
                text="Рандомный факт дня:\n{}".format(random_fact), 
                parse_mode="HTML", 
                disable_notification=True
            )

    return {
        'statusCode': 200,
    }


### Yandex Database Operations
def get_random_pepper(chat_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT * 
            FROM `peppers` 
            WHERE chat_id = {} 
            ORDER BY RANDOM(pepper_id) 
            LIMIT 1
            """.format(chat_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows[0]
        else:
            return False

    return pool.retry_operation_sync(callee)

def update_pepper_of_the_day(chat_id, user_id):
    last_updated = int(datetime.timestamp(datetime.now()))
    def callee(session):
        session.transaction().execute(
            """
            UPDATE `peppers_of_the_day`
            SET user_id = {0}, last_updated = {1}
            WHERE chat_id = {2};
            """.format(user_id, last_updated, chat_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return pool.retry_operation_sync(callee)

def get_peppers_of_the_day():
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM peppers_of_the_day;
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows
        else:
            return False

    return pool.retry_operation_sync(callee)

def get_random_fact():
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt="Расскажи рандомный факт о компьютерных играх",
        temperature=0.6,
        max_tokens=3000,
    )
    random_fact = response.choices[0].text.strip()
    print("факт: " + random_fact)
    return random_fact

if os.getenv("LAMBDA_RUNTIME_DIR") is None:
    from faker import event, context
    handler(event, context)