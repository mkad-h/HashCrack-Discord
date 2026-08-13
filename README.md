# Hashcrack Discord Adaptación de HashCrack-AI

Bot de Discord que usa **Vast.ai on-demand** + Hashcat para crackear hashes.

Arquitectura:

- El bot vive en un VPS barato / Oracle Free / tu máquina (Ideal que siempre prendido).
- Cuando llega un `/crack`, encola el job.
- El worker renta **1x RTX 4090** (Ajustable en .env), sube el hash, baja wordlist, corre Hashcat y destruye la instancia.
- Solo pagas GPU mientras se está crackeando de verdad.

Basado en la idea de HashCrack-AI de TJ Null, pero hecho para jobs asíncronos + Discord.

## Requisitos

- Linux (Testeado y desarrollado)
- Python 3.10+
- Cuenta en [Vast.ai](https://vast.ai) con créditos (Con 5 USD tienes para alrededor 12 horas continuas)
- Bot de Discord creado en https://discord.com/developers/applications

## Instalación rápida

```bash
git clone hashcrack-discord   # usando la URL
cd hashcrack-discord

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt # Sí esta en cloud, preferible hacerlo de forma segura con entorno venv

# Configura Vast CLI/SDK
pip install vastai
vastai set api-key TU_VAST_API_KEY

cp .env.example .env
nano .env   # AGREGAR DISCORD_TOKEN + ALLOWED_USER_IDS + VAST_API_KEY
```

### Crear el bot en Discord

Revisar: https://discord.com/developers/applications y luego "New Application"

### Tu Discord User ID

Para sacar el Discord User ID, se debe tener activo el modo desarollador para luego agregarlo al .env en "ALLOWED USERS".
De lo contrario si no tiene tu ID, no funcionará.


## Uso

```
/crack hash_value:{hash} mode:0 wordlist:rockyou
/status
/status job_id:{id_job}
/cancel job_id:{id_job}
/wordlists
```

## Configuración importante

En `.env`:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DEFAULT_GPU_QUERY` | `gpu_name=RTX_4090 num_gpus=1 ...` | Filtro de búsqueda de Vast. **Configurado en 1x** (ya viene así) |
| `MAX_CONCURRENT_JOBS` | `1` | más jobs en proceso (more ==+plata) |
| `MAX_JOB_MINUTES` | `90` | Timeout de 90s |
| `POLL_INTERVAL_SECONDS` | `30` | 30s de revisión |

Para hacer aún más barato (Aunque con la 4090 cuesta alrededor de 0.312 por hora (Low Cost razonable)):

```env
DEFAULT_GPU_QUERY=gpu_name=RTX_3090 num_gpus=1 verified=true rentable=true disk_space>=40
```

o incluso 3080 / 2080 Ti.

## Notas / Limitaciones

- El worker usa la CLI de `vastai` + SSH. Tiene que estar instalado y con la API key configurada.
- La primera vez genera una llave SSH (`work/hashcrack_key`) y la intenta subir a Vast.
- Wordlist `rockyou` se baja cada vez (puedes mejorar cacheando en un volumen de Vast si quieres).
- No es production-grade al 100%. Si se cae el proceso del bot a mitad de un job, la instancia puede quedar viva (Revisar Dashboard de Vast, te puede seguir cobrando)


## Estructura

```
hashcrack-discord/
├── bot.py                 # Discord bot + slash commands + poller
├── core/
│   ├── config.py
│   ├── job_manager.py     # SQLite jobs
│   └── vast_worker.py     # Lógica de renta / Hashcat / destroy
├── work/                  # temporal (hashes, llave SSH)
├── jobs.db                # se crea solo
├── .env
└── requirements.txt
```
