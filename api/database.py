# api/database.py
import os
from typing import Optional
from dotenv import load_dotenv, find_dotenv
from mysql.connector.pooling import MySQLConnectionPool
import mysql.connector

# Carrega .env do projeto (raiz ou api/.env)
env_file = find_dotenv(usecwd=True) or os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_file, override=True)  # <<--- permite sobrescrever

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Lê DB_*; se não existir, tenta MYSQL_*; senão usa default."""
    v = os.getenv(name)
    if v is None and name.startswith("DB_"):
        v = os.getenv("MYSQL_" + name[3:])
    return v if v is not None else default

DB_HOST = _env("DB_HOST", "localhost")
DB_PORT = int(_env("DB_PORT", "3306"))
DB_NAME = _env("DB_NAME", "gmdigital")
DB_USER = _env("DB_USER", "root")
DB_PASS = _env("DB_PASS", "")

# Log leve (remova depois)
print(f"[DB] usando host={DB_HOST} port={DB_PORT} user={DB_USER} db={DB_NAME}")

POOL = MySQLConnectionPool(
    pool_name="festo_pool",
    pool_size=int(_env("DB_POOL_SIZE", _env("MYSQL_POOL_SIZE", "8"))),
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    autocommit=False,
)

class DB:
    def __init__(self):
        self.conn = POOL.get_connection()
        self.cur = self.conn.cursor(dictionary=True)  # retorna dicts

    def execute(self, query: str, params=None):
        self.cur.execute(query, params or ())

    def executemany(self, query: str, seq_params):
        self.cur.executemany(query, seq_params)

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.cur.close()
        finally:
            self.conn.close()

def get_db() -> DB:
    return DB()
