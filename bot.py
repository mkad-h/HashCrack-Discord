#!/usr/bin/env python3
"""
HashCrack Discord Bot
- Reutiliza instancias Vast calientes
- Actualiza el mensaje de status solo
- Avisa cuando crackea la password
"""

import asyncio
import logging
import time
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from core.config import (
    DISCORD_TOKEN,
    ALLOWED_USER_IDS,
    MAX_CONCURRENT_JOBS,
    POLL_INTERVAL_SECONDS,
    PREDEFINED_WORDLISTS,
    INSTANCE_IDLE_MINUTES,
    REUSE_INSTANCES,
)
from core.job_manager import JobManager, JobStatus
from core.vast_worker import VastWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hashcrack-bot")

intents = discord.Intents.default()
intents.message_content = True


def format_job_message(job: dict) -> str:
    """Texto del mensaje de Discord (se edita en vivo cada 5s)."""
    import datetime

    job_id = job["job_id"]
    status = job["status"]
    mode = job.get("mode")
    wordlist = job.get("wordlist")
    inst = job.get("instance_id") or "—"
    progress = job.get("progress") or ""
    hash_preview = (job.get("hash_value") or "")[:24]
    if len(job.get("hash_value") or "") > 24:
        hash_preview += "..."

    now = datetime.datetime.now().strftime("%H:%M:%S")
    header = (
        f"**Job `{job_id}`** · `{now}`\n"
        f"• Hash: `{hash_preview}`\n"
        f"• Modo: `{mode}` | Wordlist: `{wordlist}`\n"
        f"• Instancia: `{inst}`\n"
    )

    if status == JobStatus.QUEUED.value:
        return header + f"⏳ Estado: **encolado**\n`{progress}`"
    if status == JobStatus.STARTING.value:
        return header + f"🔄 Estado: **arrancando**\n`{progress}`"
    if status == JobStatus.RUNNING.value:
        return header + f"⚡ Estado: **corriendo**\n`{progress}`"
    if status == JobStatus.CRACKED.value:
        result = job.get("result") or ""
        plain = result.rsplit(":", 1)[-1].strip() if ":" in result else result
        return header + f"✅ **CRACKEADO**\n**Password:** `{plain}`\n```\n{result}\n```"
    if status == JobStatus.CANCELLED.value:
        return header + "🛑 **Cancelado**"
    if status == JobStatus.TIMEOUT.value:
        return header + f"⏰ **Timeout**\n{job.get('error') or ''}"
    return header + f"❌ **Falló**\n```\n{(job.get('error') or 'error')[:500]}\n```"


class HashCrackBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.jm = JobManager()
        self.worker = VastWorker(self.jm)
        self._running_tasks = set()
        self._last_msg_content = {}
        self._job_messages = {}  # job_id -> discord.Message (editar sin fetch_channel)

    async def setup_hook(self):
        await self.jm.init_db()
        self.job_poller.start()
        await self.tree.sync()
        log.info("Slash commands sincronizados")

    async def on_ready(self):
        log.info(f"Logueado como {self.user} (ID: {self.user.id})")
        log.info(f"REUSE_INSTANCES={REUSE_INSTANCES} | IDLE={INSTANCE_IDLE_MINUTES}min")
        log.info(f"Usuarios permitidos: {ALLOWED_USER_IDS or 'NADIE'}")

    def is_allowed(self, user_id: int) -> bool:
        if not ALLOWED_USER_IDS:
            return False
        return user_id in ALLOWED_USER_IDS

    @tasks.loop(seconds=5.0)
    async def job_poller(self):
        """Cada 5 segundos: actualiza mensajes + cola + cleanup."""
        try:
            jobs = await self.jm.get_jobs_needing_message_update()
            for job in jobs:
                try:
                    await self._sync_job_message(job, force=True)
                except Exception as e:
                    log.warning(f"sync msg {job.get('job_id')}: {e}")

            running = await self.jm.count_running()
            if running < MAX_CONCURRENT_JOBS:
                queued = [
                    j for j in await self.jm.get_active_jobs()
                    if j["status"] == JobStatus.QUEUED.value
                ]
                for job in queued[: MAX_CONCURRENT_JOBS - running]:
                    jid = job["job_id"]
                    if jid in self._running_tasks:
                        continue
                    self._running_tasks.add(jid)
                    task = asyncio.create_task(self._run_and_cleanup(jid))
                    task.add_done_callback(lambda t, j=jid: self._running_tasks.discard(j))

            await self.worker.cleanup_idle_instances()

        except Exception as e:
            log.exception(f"Error en job_poller: {e}")

    async def _run_and_cleanup(self, job_id: str):
        try:
            await self.jm.update_job(job_id, status=JobStatus.STARTING.value, progress="Iniciando...")
            await self.worker.start_job(job_id)
        except Exception as e:
            log.exception(f"Error corriendo job {job_id}: {e}")
        finally:
            job = await self.jm.get_job(job_id)
            if job:
                await self._sync_job_message(job, force=True)

    async def _sync_job_message(self, job: dict, force: bool = False):
        """Edita el mensaje del /crack en Discord (usa cache, no fetch_channel)."""
        jid = job["job_id"]
        content = format_job_message(job)

        if not force and self._last_msg_content.get(jid) == content:
            return

        msg = self._job_messages.get(jid)

        # Fallback: intentar por channel solo si no hay cache (ej. bot reiniciado)
        if msg is None:
            mid = job.get("message_id")
            cid = job.get("channel_id")
            if not mid or not cid:
                return
            channel = self.get_channel(cid)
            if channel is None:
                # Sin acceso al canal → no spamear el log cada 5s
                if not getattr(self, "_missing_access_logged", None):
                    self._missing_access_logged = set()
                if cid not in self._missing_access_logged:
                    log.warning(
                        f"Sin acceso al canal {cid}. "
                        f"Dale al bot permiso Ver canal + Leer historial, "
                        f"o reinicia el bot y tira un /crack nuevo."
                    )
                    self._missing_access_logged.add(cid)
                return
            try:
                msg = await channel.fetch_message(int(mid))
                self._job_messages[jid] = msg
            except Exception as e:
                if not hasattr(self, "_fetch_fail_logged"):
                    self._fetch_fail_logged = set()
                if jid not in self._fetch_fail_logged:
                    log.warning(f"No pude obtener mensaje job={jid}: {e}")
                    self._fetch_fail_logged.add(jid)
                return

        try:
            await msg.edit(content=content)
            self._last_msg_content[jid] = content
        except discord.errors.NotFound:
            self._job_messages.pop(jid, None)
            log.warning(f"mensaje borrado job={jid}")
            return
        except discord.errors.HTTPException as e:
            # Rate limit u otro: no spamear
            if e.status != 429:
                log.warning(f"edit falló job={jid}: {e}")
            return
        except Exception as e:
            log.warning(f"edit falló job={jid}: {type(e).__name__}: {e}")
            return

        # Job terminado: solo marcar notified (el resultado ya quedó en ESTE mensaje editado).
        # Un solo aviso
        if job["status"] in (
            JobStatus.CRACKED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        ):
            if not job.get("notified"):
                await self.jm.update_job(job_id=jid, notified=1)
            self._job_messages.pop(jid, None)

    @job_poller.before_loop
    async def before_poller(self):
        await self.wait_until_ready()


bot = HashCrackBot()


@bot.tree.command(name="crack", description="Crackea un hash usando Vast.ai + Hashcat")
@app_commands.describe(
    hash_value="El hash a crackear",
    mode="Modo Hashcat (0=MD5, 1000=NTLM, 22000=WPA...)",
    wordlist="rockyou o URL de wordlist",
    rules="Regla opcional (best64, etc.)",
)
async def crack(
    interaction: discord.Interaction,
    hash_value: str,
    mode: int,
    wordlist: str = "rockyou",
    rules: Optional[str] = None,
):
    if not bot.is_allowed(interaction.user.id):
        await interaction.response.send_message("Sin permiso.", ephemeral=True)
        return

    running = await bot.jm.count_running()
    if running >= MAX_CONCURRENT_JOBS:
        await interaction.response.send_message(
            f"Ya hay {running} job(s) corriendo (máx {MAX_CONCURRENT_JOBS}). Usa `/cancel`.",
            ephemeral=True,
        )
        return

    warms = await bot.jm.list_warm_instances()
    warm_note = ""
    if warms:
        idle = [w for w in warms if w["status"] == "idle"]
        if idle:
            warm_note = f"\n⚡ Hay **{len(idle)}** instancia(s) caliente(s) → debería ser rápido."

    job_id = await bot.jm.create_job(
        user_id=interaction.user.id,
        channel_id=interaction.channel_id,
        hash_value=hash_value.strip(),
        mode=mode,
        wordlist=wordlist.strip(),
        rules=rules.strip() if rules else None,
    )

    # Mensaje inicial = Se actualiza cada 5s
    initial = (
        f"**Job `{job_id}`** · actualizado (esperando...)\n"
        f"• Hash: `{hash_value[:32]}{'...' if len(hash_value) > 32 else ''}`\n"
        f"• Modo: `{mode}` | Wordlist: `{wordlist}`\n"
        f"• Instancia: `—`\n"
        f"⏳ Estado: **encolado**\n"
        f"`Esperando worker...`{warm_note}"
    )
    await interaction.response.send_message(initial)
    msg = await interaction.original_response()
    await bot.jm.update_job(job_id, message_id=msg.id, channel_id=interaction.channel_id)
    # Cache en RAM: el poller edita ESTE objeto sin fetch_channel (evita 403)
    bot._job_messages[job_id] = msg
    log.info(f"job {job_id} message cacheado id={msg.id}")


@bot.tree.command(name="status", description="Estado de un job o de los tuyos")
@app_commands.describe(job_id="ID del job (opcional)")
async def status_cmd(interaction: discord.Interaction, job_id: Optional[str] = None):
    if not bot.is_allowed(interaction.user.id):
        await interaction.response.send_message("Sin permiso.", ephemeral=True)
        return

    if job_id:
        job = await bot.jm.get_job(job_id)
        if not job:
            await interaction.response.send_message("Job no encontrado.", ephemeral=True)
            return
        await interaction.response.send_message(format_job_message(job), ephemeral=True)
    else:
        jobs = await bot.jm.get_user_jobs(interaction.user.id, limit=8)
        if not jobs:
            await interaction.response.send_message("No tienes jobs.", ephemeral=True)
            return
        lines = [f"`{j['job_id']}` → **{j['status']}** | {j.get('progress') or ''}"[:100] for j in jobs]
        await interaction.response.send_message("Tus jobs:\n" + "\n".join(lines), ephemeral=True)


@bot.tree.command(name="cancel", description="Cancela un job en curso")
@app_commands.describe(job_id="ID del job")
async def cancel(interaction: discord.Interaction, job_id: str):
    if not bot.is_allowed(interaction.user.id):
        await interaction.response.send_message("Sin permiso.", ephemeral=True)
        return
    job = await bot.jm.get_job(job_id)
    if not job:
        await interaction.response.send_message("Job no existe.", ephemeral=True)
        return
    if job["user_id"] != interaction.user.id:
        await interaction.response.send_message("Ese job no es tuyo.", ephemeral=True)
        return
    ok = await bot.worker.cancel_job(job_id)
    if ok:
        await interaction.response.send_message(
            f"🛑 Job `{job_id}` cancelado. La instancia se destruye (no se reutiliza tras cancel)."
        )
    else:
        await interaction.response.send_message(f"No se pudo cancelar. Estado: `{job['status']}`")


@bot.tree.command(name="pool", description="Muestra instancias calientes en el pool")
async def pool(interaction: discord.Interaction):
    if not bot.is_allowed(interaction.user.id):
        await interaction.response.send_message("Sin permiso.", ephemeral=True)
        return
    warms = await bot.jm.list_warm_instances()
    if not warms:
        await interaction.response.send_message(
            "Pool vacío. La próxima `/crack` levantará una instancia nueva.",
            ephemeral=True,
        )
        return
    lines = []
    for w in warms:
        age = int(time.time() - (w.get("last_used_at") or 0))
        lines.append(
            f"• `{w['instance_id']}` — **{w['status']}** — "
            f"{w['ssh_host']}:{w['ssh_port']} — hace {age}s — wl=`{w.get('wordlist_path') or '—'}`"
        )
    await interaction.response.send_message(
        f"**Pool de instancias** (idle se destruye a los {INSTANCE_IDLE_MINUTES} min):\n"
        + "\n".join(lines),
        ephemeral=True,
    )


@bot.tree.command(name="destroy_pool", description="Destruye todas las instancias del pool (ahorra plata)")
async def destroy_pool(interaction: discord.Interaction):
    if not bot.is_allowed(interaction.user.id):
        await interaction.response.send_message("Sin permiso.", ephemeral=True)
        return
    warms = await bot.jm.list_warm_instances()
    if not warms:
        await interaction.response.send_message("Pool ya vacío.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    n = 0
    for w in warms:
        try:
            await bot.worker.destroy_warm_instance(w["instance_id"])
            n += 1
        except Exception as e:
            log.warning(f"destroy {w['instance_id']}: {e}")
    await interaction.followup.send(f"Destruidas **{n}** instancia(s).", ephemeral=True)


@bot.tree.command(name="wordlists", description="Wordlists predefinidas")
async def wordlists(interaction: discord.Interaction):
    names = ", ".join(f"`{k}`" for k in PREDEFINED_WORDLISTS) or "ninguna"
    await interaction.response.send_message(
        f"Predefinidas: {names}\nTambién puedes pasar una URL directa.",
        ephemeral=True,
    )


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("Falta DISCORD_TOKEN en .env")
    if not ALLOWED_USER_IDS:
        log.warning("⚠️  ALLOWED_USER_IDS vacío → nadie podrá usar el bot")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
