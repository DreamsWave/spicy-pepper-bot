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



### Main handler
def handler(event, context):
    request_body_dict = json.loads(event['body'])
    if 'message' not in request_body_dict: return
    
    update = telebot.types.Update.de_json(request_body_dict)
    bot.process_new_updates([update])
    return {
        'statusCode': 200,
    }



### Telegram commands
# /pepper
@bot.message_handler(commands=['pepper'])
def send_pepper(message):
    pepper = get_pepper(chat_id=message.chat.id, user_id=message.from_user.id)
    if pepper: # checking if we found pepper
        # if pepper exists
        now = datetime.now()
        start_of_day = datetime.timestamp(datetime(now.year,now.month,now.day))
        if pepper.last_updated < start_of_day: # checking if updated today
            # pepper hasn't updated yet
            grow_size = grow_pepper()
            new_size = pepper.size + grow_size 
            updated_pepper = update_pepper_size(
                chat_id=message.chat.id, 
                user_id=message.from_user.id, 
                size=new_size
            )
            if updated_pepper:
                msg = create_pepper_message(
                    username=message.from_user.username,
                    grow_size=grow_size,
                    place=updated_pepper.place,
                    size=updated_pepper.size
                )
                send_message(message, msg)
        else:
            # pepper already updated
            msg = create_pepper_message(
                username=message.from_user.username, 
                is_repeat=True,
                place=pepper.place,
                size=pepper.size
            )
            send_message(message, msg)
    else:
        # if no pepper found, creating new one
        grow_size = grow_pepper()
        new_pepper = create_pepper(
            chat_id=message.chat.id, 
            user_id=message.from_user.id,
            username=message.from_user.username,
            size=grow_size
        )
        if new_pepper:
            msg = create_pepper_message(
                username=message.from_user.username,
                grow_size=grow_size,
                place=new_pepper.place,
                size=new_pepper.size
            )
            send_message(message, msg)

# /top_peppers
@bot.message_handler(commands=['top_peppers'])
def send_top_peppers(message):
    top_peppers = get_top_peppers(chat_id = message.chat.id)
    if top_peppers:
        text = "Топ 10 перчиков:\n"
        for index, pepper in enumerate(top_peppers, start=1):
            text += "\n{0}| <b>{1}</b> — <b>{2} см</b>".format(index, pepper.username, pepper.size)
        send_message(message, text)
    else:
        send_message(message, "Перчики не найдены в этом чате. Введите /pepper")
    
# /pepper_of_the_day
@bot.message_handler(commands=['pepper_of_the_day'])
def send_pepper_of_the_day(message):
    pepper_of_the_day = get_pepper_of_the_day(message.chat.id)

    if pepper_of_the_day:
        # if found pepper_of_the_day in table
        now = datetime.now()
        start_of_day = datetime.timestamp(datetime(now.year,now.month,now.day))
        if pepper_of_the_day.last_updated < start_of_day:
            # if pepper_of_the_day hasn't updated yet
            random_pepper = get_random_pepper(message.chat.id)
            update_pepper_of_the_day(message.chat.id, random_pepper.user_id)
            send_message(message, "<b>@{0}</b>, поздравляю! У тебя сегодня самый лучший перчик!".format(random_pepper.username), disable_notification=False)
        else:
            # if pepper already updated today
            current_pepper_of_the_day = get_pepper(message.chat.id, pepper_of_the_day.user_id)
            if current_pepper_of_the_day:
                 # if got current_pepper_of_the_day
                send_message(message, "По результатам сегодняшнего розыгрыша лучший перчик у <b>{0}</b>!".format(current_pepper_of_the_day.username))
            else:
                # if no current_pepper_of_the_day found
                send_message(message, "Перчики не найдены в этом чате. Введите /pepper")
    else:
        # if no pepper_of_the_day found in table
        random_pepper = get_random_pepper(message.chat.id)
        if random_pepper:
            # if got random pepper
            create_pepper_of_the_day(message.chat.id, random_pepper.user_id)
            send_message(message, "<b>@{0}</b>, поздравляю! У тебя сегодня самый лучший перчик!".format(random_pepper.username))
        else:
            # if no pepper found
            send_message(message, "Перчики не найдены в этом чате. Введите /pepper")

# /ask
@bot.message_handler(commands=['ask'])
def send_ask(message):
    unique_code = extract_unique_code(message.text)
    if unique_code:
        openai.api_key = os.getenv("OPENAI_API_KEY")
        response = openai.Completion.create(
            model="text-davinci-003",
            prompt=unique_code,
            temperature=0.6,
            max_tokens=3000,
        )
        result = response.choices[0].text
        send_message(message, result)


### Yandex Database Operations
def get_pepper(chat_id, user_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM (SELECT ROW_NUMBER() OVER w AS place, peppers.*
                FROM `peppers`
                WHERE chat_id = {0}
                WINDOW w AS (ORDER BY size DESC))
            WHERE user_id = {1};
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
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM (SELECT ROW_NUMBER() OVER w AS place, peppers.*
                FROM `peppers`
                WHERE chat_id = {0}
                WINDOW w AS (ORDER BY size DESC))
            WHERE user_id = {1};
            """.format(chat_id, user_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows[0]
        else:
            return False
    return pool.retry_operation_sync(callee)

def update_pepper_size(chat_id, user_id, size):
    last_updated = int(datetime.timestamp(datetime.now()))
    def callee(session):
        session.transaction().execute(
            """
            UPDATE `peppers`
            SET size = {0}, last_updated = {1}
            WHERE chat_id = {2} AND user_id = {3};
            """.format(size, last_updated, chat_id, user_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM (SELECT ROW_NUMBER() OVER w AS place, peppers.*
                FROM `peppers`
                WHERE chat_id = {0}
                WINDOW w AS (ORDER BY size DESC))
            WHERE chat_id = {1} AND user_id = {2};
            """.format(chat_id, chat_id, user_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows[0]
        else:
            return False
    return pool.retry_operation_sync(callee)

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

def get_top_peppers(chat_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM `peppers`
            WHERE chat_id = {}
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

def get_pepper_of_the_day(chat_id):
    def callee(session):
        result_sets = session.transaction().execute(
            """
            SELECT *
            FROM peppers_of_the_day
            WHERE chat_id = {0};
            """.format(chat_id),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        if result_sets[0].rows:
            return result_sets[0].rows[0]
        else:
            return False

    return pool.retry_operation_sync(callee)

def create_pepper_of_the_day(chat_id, user_id):
    last_updated = int(datetime.timestamp(datetime.now()))
    def callee(session):
        session.transaction().execute(
            """
            UPSERT INTO `peppers_of_the_day` (chat_id, user_id, last_updated) 
            VALUES ({0}, {1}, {2});
            """.format(chat_id, user_id, last_updated),
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
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

### Utils
def grow_pepper():
    return randrange(0, 10)

def send_message(message, text, disable_notification=True):
    bot.send_message(chat_id=message.chat.id, text=text, parse_mode="HTML", disable_notification=disable_notification)

def create_pepper_message(username, size, place, grow_size=0, is_repeat=False):
    first_line = """@{}, твой перчик """.format(username)
    if is_repeat:
        first_line = """@{}, ты уже измерял перчик сегодня.\n""".format(username)
    else:
        if grow_size > 0:
            first_line += "вырос на <b>{} см</b>.\n".format(grow_size)
        elif grow_size == 0:
            first_line += "не изменился.\n"
        else:
            first_line += "уменьшился на <b>{} см</b>.\n".format(abs(grow_size))
    second_line = """{0} он равен <b>{1} см</b>.\n""".format("Сейчас" if is_repeat else "Теперь", size)
    third_line = """Ты занимаешь <b>{} место</b> в топе.\n""".format(place)
    fourth_line = """Следующая попытка завтра!"""
    return first_line + second_line + third_line + fourth_line

def extract_unique_code(text):
    return ' '.join(text.split()[1:]) if len(text.split()) > 1 else None

if os.getenv("LAMBDA_RUNTIME_DIR") is None:
    from faker import event, context
    handler(event, context)