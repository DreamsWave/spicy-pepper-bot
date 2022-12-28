import logging
import telebot
import json
import os
import ydb
import ydb.iam
from datetime import datetime
from random import randrange
import uuid
from dotenv import load_dotenv

load_dotenv()

logger = telebot.logger
telebot.logger.setLevel(logging.INFO)

bot = telebot.TeleBot(os.getenv('TELEGRAM_TOKEN'), threaded=False, parse_mode="HTML")

global_pepper = dict()

# create driver in global space.
driver = ydb.Driver(
        endpoint = os.getenv("YDB_ENDPOINT"), 
        database = os.getenv("YDB_DATABASE"),
        credentials = ydb.iam.ServiceAccountCredentials.from_file(os.getenv("SA_KEY_FILE")) if os.getenv("LAMBDA_RUNTIME_DIR") is None else None
    )
# Wait for the driver to become active for requests.
driver.wait(timeout=15)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

def handler(event, context):
    request_body_dict = json.loads(event['body'])
    if 'message' not in request_body_dict: return

    global global_pepper
    global_pepper["chat_id"] = request_body_dict['message']['chat']['id']
    global_pepper["user_id"] = request_body_dict['message']['from']['id']
    global_pepper["username"] = request_body_dict['message']['from']['username']
    # print(chat_id, user_id, username)

    update = telebot.types.Update.de_json(request_body_dict)
    bot.process_new_updates([update])
    return {
        'statusCode': 200,
    }

### Telegram commands
# /pepper
@bot.message_handler(commands=['pepper'])
def send_pepper(message):
    print('pepper command')
    pepper = get_pepper(chat_id=global_pepper["chat_id"], user_id=global_pepper["user_id"])
    if pepper:
        print(pepper)
        now = datetime.now()
        start_of_day = datetime.timestamp(datetime(now.year,now.month,now.day))
        if pepper.last_updated < start_of_day:
            print("Pepper hasn't updated yet")
            grow_by = grow_pepper()
            new_size = pepper.size + grow_by
            update_pepper_size(pepper_id=pepper.pepper_id, size=new_size)
            print("Pepper updated by {0}. It's {1} now".format(grow_by, new_size))
            send_message(message, "Pepper updated by {0}. It's {1} now".format(grow_by, new_size))
        else:
            print("Pepper already updated today. It's {0}".format(pepper.size))
            send_message(message, "Pepper has already updated today. It's {0}".format(pepper.size))
    else:
        print('No pepper found')
        grow_by = grow_pepper()
        create_pepper(
            chat_id=global_pepper['chat_id'], 
            user_id=global_pepper['user_id'],
            username=global_pepper['username'],
            size=grow_by
        )
        print("Pepper created. It's {0} now".format(grow_by))
        send_message(message, "Pepper created. It's {0} now".format(grow_by))

# /top_peppers
@bot.message_handler(commands=['top_peppers'])
def send_top_peppers(message):
    print('top_peppers command')
    top_peppers = get_top_peppers(chat_id = message.chat.id)
    print(top_peppers)
    if top_peppers:
        print('Peppers found')
        text = "Top 10 peppers:\n"
        for index, pepper in enumerate(top_peppers, start=1):
            text += "\n{0}| <b>{1}</b> — <b>{2}</b> cm".format(index, message.from_user.first_name, pepper.size)
        send_message(message, text)
    else:
        print('No peppers found')
        send_message(message, "No peppers found")
    
# /pepper_of_the_day
@bot.message_handler(commands=['pepper_of_the_day'])
def send_pepper_of_the_day(message):
    print('pepper_of_the_day command')


# Yandex Database Operations
def get_pepper(chat_id, user_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM peppers
            WHERE chat_id = {0} AND user_id = {1};
            """.format(chat_id, user_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows[0]
        else:
            return False

    return pool.retry_operation_sync(callee)
    
def create_pepper(chat_id, user_id, username, size):
    pepper_id = uuid.uuid4()
    last_updated = int(datetime.timestamp(datetime.now()))
    def callee(session):
        session.transaction().execute(
            """
            UPSERT INTO `peppers` (pepper_id, chat_id, user_id, username, size, last_updated) 
            VALUES ("{0}", {1}, {2}, "{3}", {4}, {5});
            """.format(pepper_id, chat_id, user_id, username, size, last_updated),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return pool.retry_operation_sync(callee)

def update_pepper_size(pepper_id, size):
    last_updated = int(datetime.timestamp(datetime.now()))
    print(pepper_id, size, last_updated)
    def callee(session):
        session.transaction().execute(
            """
            UPDATE `peppers`
            SET size = {0}, last_updated = {1}
            WHERE pepper_id = "{2}";
            """.format(size, last_updated, pepper_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return pool.retry_operation_sync(callee)

def get_random_pepper(chat_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT * 
            FROM `peppers` 
            WHERE chat_id == {} 
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

def get_top_peppers(chat_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM `peppers`
            WHERE chat_id == {}
            ORDER BY size DESC
            LIMIT 10
            """.format(chat_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows
        else:
            return False

    return pool.retry_operation_sync(callee)

# Utils
def grow_pepper():
    return randrange(-3, 20)
def send_message(message, text):
    bot.send_message(chat_id=message.chat.id, text=text, parse_mode="HTML")



if os.getenv("LAMBDA_RUNTIME_DIR") is None:
    from faker import event, context
    handler(event, context)