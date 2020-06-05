import os

from dotenv import load_dotenv


# 加载配置
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), encoding='utf8')
mongo_uri = os.getenv('MONGO_URI')
mongo_database = os.getenv('MONGO_DATABASE')

if __name__ == "__main__":
    print(mongo_database, mongo_uri)