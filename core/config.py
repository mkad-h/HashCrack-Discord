import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
VAST_API_KEY = os.getenv("VAST_API_KEY", "")

ALLOWED_USER_IDS = {
    int(x.strip()) for x in os.getenv("ALLOWED_USER_IDS", "").split(",") if x.strip().isdigit()
}

DEFAULT_GPU_QUERY = os.getenv(
    "DEFAULT_GPU_QUERY",
    "gpu_name=RTX_4090 num_gpus=1 verified=true rentable=true disk_space>=20 inet_down>=500 reliability>=0.95"
)

MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "1"))
MAX_JOB_MINUTES = int(os.getenv("MAX_JOB_MINUTES", "90"))
# Cada cuánto se actualiza el mensaje de Discord (segundos)
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))

PREDEFINED_WORDLISTS = {
    "rockyou": "https://github.com/brannondorsey/naive-hashcat/releases/download/data/rockyou.txt", # Puedes agregar más wordlists
}

HASHCAT_IMAGE = "dizcza/docker-hashcat:cuda"

WORK_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "work")
os.makedirs(WORK_DIR, exist_ok=True)

SSH_READY_MAX_WAIT = int(os.getenv("SSH_READY_MAX_WAIT", "150"))
SSH_POLL_INTERVAL = float(os.getenv("SSH_POLL_INTERVAL", "3.0"))
WORDLIST_READY_MAX_WAIT = int(os.getenv("WORDLIST_READY_MAX_WAIT", "60"))
DISK_GB = int(os.getenv("DISK_GB", "25"))

# Minutos idle antes de destruir una warm (default alto para no recrear a cada rato)
INSTANCE_IDLE_MINUTES = int(os.getenv("INSTANCE_IDLE_MINUTES", "45"))
# Si True, reutiliza instancia existente y NO destruye al terminar el job
REUSE_INSTANCES = os.getenv("REUSE_INSTANCES", "true").lower() in ("1", "true", "yes")
