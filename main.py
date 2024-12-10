import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query
import csv
from traceback import print_stack

from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv('HOST')
port = os.getenv('PORT')
db = os.getenv('DB')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')


class Redis_Client:
    def __init__(self):

        self.redis = None
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

    def connect(self):

        try:
            self.redis = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                username=self.username,
                password=self.password,
            )
            print("Connected to Redis.")
        except Exception as e:
            print("Failed to connect to Redis.")
            print(e)
            print_stack()

    def load_users(self, file_path):
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    # Parse the user data
                    user_data = self.parse_line(line)

                    if user_data:
                        user_id = user_data.pop("user_id")  # Extract user_id to use as the Redis key
                        self.redis.hset(user_id, mapping=user_data)  # Store user data as a hash
                        
            print("User data successfully loaded into Redis.")
        except Exception as e:
            print(f"Error loading users: {e}")

    def parse_line(self, line):
        fields = line.strip().replace('"', '').split(' ')
        if not fields or len(fields) < 2:
            return None

        user_data = {}
        user_data["user_id"] = fields[0]
        for i in range(1, len(fields), 2):
            if i + 1 < len(fields):
                key = fields[i]
                value = fields[i + 1]
                user_data[key] = value
        return user_data

    def query1(self, usr):
        """
        Return all attributes of the user by usr.
        """
        try:
            result = self.redis.hgetall(f"user:{usr}")
            print(f"Query 1 result: {result}")
            return result
        except Exception as e:
            print("Error executing query 1.")
            print(e)

    def query2(self, usr):
        """
        Return the coordinate (longitude and latitude) of the user by usr.
        """
        try:
            longitude = self.redis.hget(f"user:{usr}", "longitude")
            latitude = self.redis.hget(f"user:{usr}", "latitude")
            coordinates = (longitude, latitude)
            print(f"Query 2 result: {coordinates}")
            return coordinates
        except Exception as e:
            print("Error executing query 2.")
            print(e)

    def query3(self):
        """
        Get keys and last names of users whose ids do not start with an odd number.
        """
        try:
            cursor = 1280
            user_ids = []
            last_names = []
            while cursor != 0:
                cursor, keys = self.redis.scan(cursor=cursor, count=100)
                for key in keys:
                    if not int(key.decode("utf-8").split(":")[1]) % 2 == 1:
                        user_ids.append(key)
                        last_names.append(self.redis.hget(key, "last_name"))
            print(f"Query 3 result: {user_ids}, {last_names}")
            return user_ids, last_names
        except Exception as e:
            print("Error executing query 3.")
            print(e)

    def query4(self):
        """
        Return females in China or Russia with latitude between 40 and 46.
        """
        try:
            # Create secondary index
            index_name = "user_index"
            self.redis.ft(index_name).create_index(
                [
                    TextField("gender"),
                    TagField("country"),
                    NumericField("latitude"),
                ],
                definition=IndexDefinition(prefix=["user:"]),
            )
            query = Query("@gender:female @country:{China|Russia} @latitude:[40 46]")
            result = self.redis.ft(index_name).search(query)
            print("Query 4 result:")
            for doc in result.docs:
                print(doc)
            return result.docs
        except Exception as e:
            print("Error executing query 4.")
            print(e)

    def query5(self):
        """
        Get email ids of the top 10 players in leaderboard:2.
        """
        try:
            top_players = self.redis.zrevrange("leaderboard", 0, 9, withscores=True)
            emails = [self.redis.hget(f"user:{player[0].decode('utf-8')}", "email") for player in top_players]
            print(f"Query 5 result: {emails}")
            return emails
        except Exception as e:
            print("Error executing query 5.")
            print(e)
    

        

# Initialize Redis Client and execute queries
if __name__ == "__main__":

    rs = Redis_Client()
    rs.connect()
    rs.load_users("./datasets/users.txt")
    # rs.load_scores("./datasets/scores.txt")
    # rs.query1(299)
    # rs.query2(2836)
    # rs.query3()
    # rs.query4()
    # rs.query5()

