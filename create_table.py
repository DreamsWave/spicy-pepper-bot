import os
import ydb
import ydb.iam
from dotenv import load_dotenv

load_dotenv()

# # create driver in global space.
# driver = ydb.Driver(
#     endpoint=os.getenv("YDB_ENDPOINT"), 
#     database=os.getenv("YDB_DATABASE"),
#     credentials=ydb.iam.ServiceAccountCredentials.from_file(os.getenv("SA_KEY_FILE"))
# )
# # Wait for the driver to become active for requests.
# driver.wait(timeout=15)
# # Create the session pool instance to manage YDB sessions.
# pool = ydb.SessionPool(driver)

def create_tables(pool):
    def callee(session):
        session.execute_scheme(
            """
                CREATE table `peppers` (
                    `pepper_id` Utf8,
                    `chat_id` Int64,
                    `user_id` Int64,
                    `username` Utf8,
                    `size` Int64,
                    `last_updated` Int64,
                    PRIMARY KEY (`pepper_id`)
                )
                """
        )
    return pool.retry_operation_sync(callee)

def run():
    with ydb.Driver(endpoint=os.getenv("YDB_ENDPOINT"), database=os.getenv("YDB_DATABASE"), credentials=ydb.iam.ServiceAccountCredentials.from_file(os.getenv("SA_KEY_FILE"))) as driver:
        driver.wait(timeout=5, fail_fast=True)

        with ydb.SessionPool(driver) as pool:

            create_tables(pool)
run()