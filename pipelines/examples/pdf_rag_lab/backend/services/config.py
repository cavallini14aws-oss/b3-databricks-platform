import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
VECTORSTORE_DIR = BASE_DIR / "vectorstore"

DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_TOP_K = 4
DEFAULT_RETRIEVAL_CANDIDATES = 10
DEFAULT_CHUNK_SIZE = 1200
DEFAULT_CHUNK_OVERLAP = 200
MIN_CHUNK_LENGTH = 120

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
DEFAULT_LLM_MODEL = os.getenv("DEFAULT_LLM_MODEL", "llama3.1:8b")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_SERVING_ENDPOINT = os.getenv("DATABRICKS_SERVING_ENDPOINT", "")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")

RAW_DIR.mkdir(parents=True, exist_ok=True)
VECTORSTORE_DIR.mkdir(parents=True, exist_ok=True)
