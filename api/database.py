# api/database.py
import os
from typing import Optional, Iterable, Any, Tuple, List, Dict

from dotenv import load_dotenv, find_dotenv
import mysql.connector
from mysql.connector import pooling

# -------------------------------------------------
# Carrega .env (raiz do projeto e api/.env)
# -------------------------------------------------
# 1) procura um .env "de verdade" (raiz)
env_root = find_dotenv(usecwd=True)
if env_root:
    load_dotenv(env_root, override=False)
# 2) também carrega api/.env (pode sobrescrever se desejar)
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

# -------------------------------------------------
# Helpers para ler env aceitando DB_* e MYSQL_*
# -------------------------------------------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None and name.startswith("DB_"):
        # aceita também MYSQL_*
        v = os.getenv("MYSQL_" + name[3:])
    return v if v is not None else default

DB_HOST = _env("DB_HOST", "127.0.0.1")
DB_PORT = int(_env("DB_PORT", "3306") or "3306")
DB_NAME = _env("DB_NAME", "gmdigital")
DB_USER = _env("DB_USER", "root")
DB_PASS = _env("DB_PASS", "")

# Tamanho/nome do pool
MYSQL_POOL_NAME = os.getenv("MYSQL_POOL_NAME", "backend")
MYSQL_POOL_SIZE = int(os.getenv("MYSQL_POOL_SIZE", "30") or "30")
MYSQL_POOL_RESET_SESSION = os.getenv("MYSQL_POOL_RESET_SESSION", "true").lower() == "true"

# Log curtinho (igual ao que você já usa)
print(f"[DB] usando host={DB_HOST} port={DB_PORT} user={DB_USER} db={DB_NAME}")

# -------------------------------------------------
# Cria Pool (autocommit=True para evitar conexões penduradas)
# -------------------------------------------------
POOL: pooling.MySQLConnectionPool = pooling.MySQLConnectionPool(
    pool_name=MYSQL_POOL_NAME,
    pool_size=MYSQL_POOL_SIZE,
    pool_reset_session=MYSQL_POOL_RESET_SESSION,
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    autocommit=True,            # muito importante para não deixar transação aberta
    connection_timeout=10,      # ajuda a não travar ao pegar conexão
)

class DB:
    """
    Wrapper de conexão:
      - usa cursor dictionary=True (rows como dict)
      - devolve conexão ao pool no close()
      - pode ser usado com 'with get_db() as db: ...'
    """
    def __init__(self):
        self.conn = POOL.get_connection()
        self.cur = self.conn.cursor(dictionary=True)

    # API básica
    def execute(self, sql: str, params: Tuple[Any, ...] | None = None) -> None:
        self.cur.execute(sql, params or ())

    def executemany(self, sql: str, seq_params: Iterable[Tuple[Any, ...]]) -> None:
        self.cur.executemany(sql, list(seq_params))

    def fetchone(self) -> Optional[Dict[str, Any]]:
        return self.cur.fetchone()

    def fetchall(self) -> List[Dict[str, Any]]:
        return self.cur.fetchall()

    def commit(self) -> None:
        # autocommit já está ON, mas manter esse método não machuca
        try:
            self.conn.commit()
        except Exception:
            # se autocommit, commit aqui geralmente é no-op
            pass

    def close(self) -> None:
        # fecha cursor e devolve a conexão ao pool
        try:
            try:
                if self.cur:
                    self.cur.close()
            finally:
                if self.conn:
                    self.conn.close()
        finally:
            self.cur = None
            self.conn = None

    # Context manager
    def __enter__(self) -> "DB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def get_db() -> DB:
    """Retorna uma conexão 'descartável'. Use com 'with' ou chame close()."""
    return DB()

# -------------------------------------------------
# Helpers de query (compat com seu código atual)
# -------------------------------------------------
def fetch_one(sql: str, params: Tuple[Any, ...] | None = None) -> Optional[Dict[str, Any]]:
    with get_db() as db:
        db.execute(sql, params)
        return db.fetchone()

def fetch_all(sql: str, params: Tuple[Any, ...] | None = None) -> List[Dict[str, Any]]:
    with get_db() as db:
        db.execute(sql, params)
        return db.fetchall()

def execute(sql: str, params: Tuple[Any, ...] | None = None, commit: bool = True) -> None:
    with get_db() as db:
        db.execute(sql, params)
        if commit:
            db.commit()

def executemany(sql: str, seq_params: Iterable[Tuple[Any, ...]], commit: bool = True) -> None:
    with get_db() as db:
        db.executemany(sql, seq_params)
        if commit:
            db.commit()
