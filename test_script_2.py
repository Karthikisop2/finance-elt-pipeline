import os
from dotenv import load_dotenv

load_dotenv()

print("USER:", os.getenv("POSTGRES_USER"))
print("DB:", os.getenv("POSTGRES_DB"))