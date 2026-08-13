"""
Worker optimizado para arranque rápido 

Estrategia:
1. onstart-cmd descarga la wordlist MIENTRAS la instancia bootea
2. Poll SSH cada 2s, sin sleep inicial
3. Sin apt / sin tmux (usa nohup)
4. Disco chico, query con buen inet_down
"""

import asyncio
import json
import os
import re
import shlex
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

from .config import (
    DEFAULT_GPU_QUERY,
    HASHCAT_IMAGE,
    WORK_DIR,
    MAX_JOB_MINUTES,
    PREDEFINED_WORDLISTS,
    SSH_READY_MAX_WAIT,
    SSH_POLL_INTERVAL,
    WORDLIST_READY_MAX_WAIT,
    DISK_GB,
    REUSE_INSTANCES,
    INSTANCE_IDLE_MINUTES,
)
from .job_manager import JobManager, JobStatus


def run_cmd(cmd: str, timeout: int = 120, capture: bool = True) -> Tuple[int, str, str]:
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=capture, text=True, timeout=timeout
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as e:
        return -1, "", str(e)


def strip_ansi(s: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", s or "")


class VastWorker:
    def __init__(self, job_manager: JobManager):
        self.jm = job_manager
        self.ssh_key_path = Path(WORK_DIR) / "hashcrack_key"
        self._ensure_ssh_key()

    def _ensure_ssh_key(self):
        if self.ssh_key_path.exists():
            return
        run_cmd(f'ssh-keygen -t rsa -f "{self.ssh_key_path}" -q -N ""')
        run_cmd(f'chmod 600 "{self.ssh_key_path}" "{self.ssh_key_path}.pub"')
        pub = self.ssh_key_path.with_suffix(".pub").read_text().strip()
        run_cmd(f'vastai create ssh-key "{pub}"', timeout=30)

    async def start_job(self, job_id: str) -> None:
        await self.jm.update_job(job_id, status=JobStatus.STARTING.value, started_at=time.time())
        try:
            await asyncio.to_thread(self._run_job_sync, job_id)
        except Exception as e:
            await self.jm.update_job(
                job_id,
                status=JobStatus.FAILED.value,
                error=str(e)[:500],
                finished_at=time.time(),
            )

    def _get_job_sync(self, job_id: str) -> Optional[dict]:
        conn = sqlite3.connect(self.jm.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def _release_warm_sync(self, instance_id: str, host: str, port: int, wordlist_path: str = ""):
        now = time.time()
        conn = sqlite3.connect(self.jm.db_path)
        existing = conn.execute(
            "SELECT 1 FROM warm_instances WHERE instance_id=?", (instance_id,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE warm_instances SET status='idle', last_used_at=?, ssh_host=?, ssh_port=?, wordlist_path=? WHERE instance_id=?",
                (now, host, port, wordlist_path or "", instance_id),
            )
        else:
            conn.execute(
                """INSERT INTO warm_instances (instance_id, ssh_host, ssh_port, wordlist_path, status, last_used_at, created_at)
                   VALUES (?, ?, ?, ?, 'idle', ?, ?)""",
                (instance_id, host, port, wordlist_path or "", now, now),
            )
        conn.commit()
        conn.close()
        print(f"[vast] pool ← idle instancia {instance_id} ({host}:{port})")

    def _remove_warm_sync(self, instance_id: str):
        conn = sqlite3.connect(self.jm.db_path)
        conn.execute("DELETE FROM warm_instances WHERE instance_id=?", (instance_id,))
        conn.commit()
        conn.close()

    def _mark_warm_busy_sync(self, instance_id: str):
        conn = sqlite3.connect(self.jm.db_path)
        conn.execute(
            "UPDATE warm_instances SET status='busy', last_used_at=? WHERE instance_id=?",
            (time.time(), instance_id),
        )
        conn.commit()
        conn.close()

    def _ssh_alive(self, host: str, port: int) -> bool:
        rc, out, _ = run_cmd(
            f'ssh -i "{self.ssh_key_path}" -o StrictHostKeyChecking=no '
            f'-o UserKnownHostsFile=/dev/null -o ConnectTimeout=6 -o BatchMode=yes '
            f'-o IdentitiesOnly=yes -p {port} root@{host} "echo ok"',
            timeout=15,
        )
        return rc == 0 and "ok" in (out or "")

    def _list_vast_running(self) -> list:
        """Instancias running en la cuenta Vast (fuente de verdad)."""
        rc, out, err = run_cmd("vastai show instances --raw", timeout=30)
        if rc != 0:
            print(f"[vast] show instances falló: {err or out}")
            return []
        try:
            data = json.loads(strip_ansi(out))
        except Exception as e:
            print(f"[vast] parse show instances: {e}")
            return []
        if not isinstance(data, list):
            return []
        running = []
        for inst in data:
            status = str(
                inst.get("actual_status")
                or inst.get("cur_state")
                or inst.get("status")
                or ""
            ).lower()
            # loading también puede servir en un momento, pero preferimos running
            if status in ("running", "loading", "created"):
                running.append(inst)
        return running

    def _active_job_instance_ids(self) -> set:
        """instance_ids usados por jobs queued/starting/running ahora."""
        conn = sqlite3.connect(self.jm.db_path)
        rows = conn.execute(
            """
            SELECT instance_id FROM jobs
            WHERE status IN ('queued', 'starting', 'running')
              AND instance_id IS NOT NULL AND instance_id != ''
            """
        ).fetchall()
        conn.close()
        return {str(r[0]) for r in rows if r[0]}

    def _recover_stale_busy(self):
        """busy sin job activo → vuelve a idle (evita pool muerto)."""
        active = self._active_job_instance_ids()
        conn = sqlite3.connect(self.jm.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT instance_id FROM warm_instances WHERE status='busy'"
        ).fetchall()
        for r in rows:
            iid = str(r["instance_id"])
            if iid not in active:
                conn.execute(
                    "UPDATE warm_instances SET status='idle' WHERE instance_id=?",
                    (iid,),
                )
                print(f"[vast] pool: busy stale {iid} → idle")
        conn.commit()
        conn.close()

    def _sync_vast_into_pool(self):
        """Mete al pool cualquier instancia running de Vast que no esté registrada."""
        vast_insts = self._list_vast_running()
        if not vast_insts:
            return
        conn = sqlite3.connect(self.jm.db_path)
        known = {
            str(r[0])
            for r in conn.execute("SELECT instance_id FROM warm_instances").fetchall()
        }
        conn.close()
        for inst in vast_insts:
            iid = str(inst.get("id") or "")
            if not iid or iid in known:
                continue
            # Solo nos interesan las de nuestra imagen hashcat si se puede filtrar
            image = str(inst.get("image_uuid") or inst.get("image") or "").lower()
            # Si no podemos saber la imagen, igual la consideramos candidata
            try:
                host, port = self._get_ssh_info(iid)
            except Exception as e:
                print(f"[vast] no SSH info para {iid}: {e}")
                continue
            self._release_warm_sync(iid, host, port, "/root/wordlist.txt")
            print(f"[vast] pool ← descubierta en Vast {iid} ({host}:{port}) image={image[:40]}")

    def _try_reuse_candidate(self, instance_id: str, host: str, port: int, wordlist_path: str = "") -> Optional[Tuple[str, str, int, str]]:
        """Prueba SSH (refrescando endpoint si hace falta). None si muerta."""
        # 1er intento con datos del pool
        if host and port and self._ssh_alive(host, port):
            self._mark_warm_busy_sync(instance_id)
            return instance_id, host, int(port), wordlist_path or ""

        # 2do intento: refrescar ssh-url desde Vast
        try:
            host2, port2 = self._get_ssh_info(instance_id)
            print(f"[vast] SSH endpoint refrescado {instance_id}: {host2}:{port2}")
            if self._ssh_alive(host2, port2):
                self._release_warm_sync(instance_id, host2, port2, wordlist_path or "")
                self._mark_warm_busy_sync(instance_id)
                return instance_id, host2, int(port2), wordlist_path or ""
        except Exception as e:
            print(f"[vast] refresh SSH {instance_id} falló: {e}")

        # Muerta → sacar del pool (NO destruir automáticamente aquí si Vast dice running;
        # el caller decide). Por defecto la sacamos del pool local.
        print(f"[vast] candidata {instance_id} no responde SSH → fuera del pool")
        self._remove_warm_sync(instance_id)
        return None

    def _find_existing_instance(self) -> Optional[Tuple[str, str, int, str]]:
        """
        Busca UNA instancia usable ANTES de crear.
        Orden:
          1) recuperar busy stale
          2) sincronizar running de Vast → pool
          3) probar todas las del pool (idle primero)
        """
        if not REUSE_INSTANCES:
            return None

        self._recover_stale_busy()
        try:
            self._sync_vast_into_pool()
        except Exception as e:
            print(f"[vast] sync pool desde Vast: {e}")

        conn = sqlite3.connect(self.jm.db_path)
        conn.row_factory = sqlite3.Row
        # idle primero, luego cualquier otra
        rows = conn.execute(
            """
            SELECT * FROM warm_instances
            ORDER BY CASE status WHEN 'idle' THEN 0 ELSE 1 END, last_used_at DESC
            """
        ).fetchall()
        conn.close()

        active = self._active_job_instance_ids()
        print(f"[vast] pool tiene {len(rows)} instancia(s); jobs activos con instancia: {active or 'ninguno'}")

        for row in rows:
            inst = dict(row)
            iid = str(inst["instance_id"])
            if iid in active:
                print(f"[vast] skip {iid}: ya usada por otro job activo")
                continue
            print(f"[vast] probando pool {iid} status={inst.get('status')} {inst.get('ssh_host')}:{inst.get('ssh_port')}")
            result = self._try_reuse_candidate(
                iid,
                inst.get("ssh_host") or "",
                int(inst.get("ssh_port") or 0),
                inst.get("wordlist_path") or "",
            )
            if result:
                print(f"[vast] REUSE OK → {result[0]} @ {result[1]}:{result[2]}")
                return result

        # Último recurso: mirar Vast running aunque no haya entrado al pool
        for inst in self._list_vast_running():
            iid = str(inst.get("id") or "")
            if not iid or iid in active:
                continue
            status = str(inst.get("actual_status") or inst.get("cur_state") or "").lower()
            if status != "running":
                continue
            try:
                host, port = self._get_ssh_info(iid)
            except Exception:
                continue
            result = self._try_reuse_candidate(iid, host, port, "/root/wordlist.txt")
            if result:
                print(f"[vast] REUSE desde Vast directo → {result[0]}")
                return result

        print("[vast] no hay instancia reutilizable → hay que crear")
        return None

    def _run_job_sync(self, job_id: str):
        t0 = time.time()
        job = self._get_job_sync(job_id)
        if not job:
            return

        hash_value = job["hash_value"]
        mode = job["mode"]
        wordlist = job["wordlist"]
        rules = job["rules"]

        hash_file = Path(WORK_DIR) / f"{job_id}.hash"
        hash_file.write_text(hash_value.strip() + "\n")

        if wordlist in PREDEFINED_WORDLISTS:
            wl_url = PREDEFINED_WORDLISTS[wordlist]
            wl_remote = "/root/wordlist.txt"
        else:
            wl_url = wordlist
            fname = wordlist.split("/")[-1].split("?")[0] or "wordlist.txt"
            wl_remote = f"/root/{fname}"

        instance_id = None
        host = None
        port = None
        reused = False
        final_wl = wl_remote

        try:
            self._update_sync(job_id, progress="Buscando instancia existente...")

            # 1) SIEMPRE buscar existente (pool + Vast) antes de crear
            found = self._find_existing_instance()
            if found:
                instance_id, host, port, prev_wl = found
                reused = True
                if prev_wl:
                    final_wl = prev_wl
                self._update_sync(
                    job_id,
                    progress=f"⚡ Reutilizando instancia `{instance_id}`",
                    instance_id=instance_id,
                    ssh_host=host,
                    ssh_port=port,
                )
                print(f"[vast] [{job_id}] REUSE {instance_id} @ {host}:{port} | t={time.time()-t0:.0f}s")
            else:
                # 2) Solo aquí se crea una nueva
                self._update_sync(job_id, progress="No hay instancia viva → creando en Vast.ai...")
                print(f"[vast] [{job_id}] CREATE nueva (no había reutilizable) | t={time.time()-t0:.0f}s")
                instance_id, host, port = self._create_instance(wl_url, wl_remote)
                self._update_sync(
                    job_id,
                    instance_id=instance_id,
                    ssh_host=host,
                    ssh_port=port,
                    progress=f"Instancia `{instance_id}` creada, esperando SSH...",
                )
                self._wait_instance_ready(instance_id, host, port, max_wait=SSH_READY_MAX_WAIT)
                self._update_sync(job_id, progress="SSH OK. Esperando wordlist...")
                final_wl = self._wait_wordlist(host, port, wl_remote, max_wait=WORDLIST_READY_MAX_WAIT)
                # Registrar en pool de inmediato (busy mientras corre el job)
                self._release_warm_sync(instance_id, host, port, final_wl)
                self._mark_warm_busy_sync(instance_id)
                print(f"[vast] [{job_id}] instancia nueva lista {instance_id} | t={time.time()-t0:.0f}s")

            # Asegurar wordlist (tanto reuse como nueva ya cubierta; reuse puede necesitarla)
            if reused:
                need_wl = True
                if final_wl:
                    chk = self._ssh(host, port, f"test -s {shlex.quote(final_wl)} && echo YES || echo NO")
                    need_wl = "YES" not in (chk or "")
                if need_wl:
                    # probar path canónico
                    chk2 = self._ssh(host, port, "test -s /root/wordlist.txt && echo YES || echo NO")
                    if "YES" in (chk2 or ""):
                        final_wl = "/root/wordlist.txt"
                        need_wl = False
                if need_wl:
                    self._update_sync(job_id, progress="Bajando wordlist en instancia reutilizada...")
                    self._ssh(
                        host, port,
                        f"curl -fsSL -o /root/wordlist.txt {shlex.quote(wl_url)} "
                        f"|| wget -q -O /root/wordlist.txt {shlex.quote(wl_url)}; "
                        f"if file /root/wordlist.txt 2>/dev/null | grep -qi gzip; then "
                        f"mv /root/wordlist.txt /root/wordlist.txt.gz && gunzip -f /root/wordlist.txt.gz; fi",
                        timeout=300,
                    )
                    final_wl = self._wait_wordlist(host, port, "/root/wordlist.txt", max_wait=WORDLIST_READY_MAX_WAIT)

            self._update_sync(job_id, status=JobStatus.RUNNING.value, progress="Subiendo hash + lanzando Hashcat...")

            # Subir hash
            self._scp_to(host, port, str(hash_file), f"/root/{job_id}.hash")

            # Matar hashcat anterior si quedó colgado
            self._ssh(host, port, "pkill -9 hashcat 2>/dev/null || true", timeout=15)

            rules_arg = f"-r {shlex.quote(rules)}" if rules else ""
            if final_wl.endswith(".gz"):
                self._ssh(host, port, f"gunzip -f {final_wl}", timeout=60)
                final_wl = final_wl[:-3]

            hashcat_cmd = (
                f"nohup hashcat -a 0 -m {mode} -O -w 3 "
                f"/root/{job_id}.hash {final_wl} {rules_arg} "
                f"-o /root/{job_id}.cracked --status --status-timer=10 "
                f"> /root/{job_id}.log 2>&1 &"
            )
            self._ssh(host, port, hashcat_cmd, timeout=30)
            print(f"[vast] [{job_id}] Hashcat lanzado (reuse={reused}) | t={time.time()-t0:.0f}s")
            self._update_sync(
                job_id,
                progress=f"Hashcat corriendo{' (instancia reutilizada ⚡)' if reused else ''}...",
            )

            self._monitor(job_id, instance_id, host, port, final_wl, max_minutes=MAX_JOB_MINUTES)

        except Exception as e:
            self._update_sync(
                job_id,
                status=JobStatus.FAILED.value,
                error=str(e)[:500],
                progress=f"Falló: {str(e)[:120]}",
                finished_at=time.time(),
            )
            # Si falló al crear/usar, no dejar basura en pool
            if instance_id and not reused:
                self._destroy_instance(instance_id)
                self._remove_warm_sync(instance_id)
            elif instance_id and reused:
                # warm falló a mitad: soltar o destruir según gravedad
                try:
                    if self._ssh_alive(host, port):
                        self._release_warm_sync(instance_id, host, port, final_wl or "")
                    else:
                        self._destroy_instance(instance_id)
                        self._remove_warm_sync(instance_id)
                except Exception:
                    pass
            raise
        finally:
            try:
                hash_file.unlink(missing_ok=True)
            except Exception:
                pass

    def _update_sync(self, job_id: str, **kwargs):
        kwargs["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [job_id]
        conn = sqlite3.connect(self.jm.db_path)
        conn.execute(f"UPDATE jobs SET {cols} WHERE job_id = ?", values)
        conn.commit()
        conn.close()

    def _create_instance(self, wordlist_url: str, remote_path: str) -> Tuple[str, str, int]:
        """Crea instancia y deja bajando la wordlist en onstart (paralelo al boot)."""
        search_cmd = (
            f"vastai search offers '{DEFAULT_GPU_QUERY}' "
            f"--order 'dph_total' --limit 15 --raw"
        )
        rc, out, err = run_cmd(search_cmd, timeout=40)
        if rc != 0:
            raise RuntimeError(f"No se pudo buscar ofertas: {err or out}")

        out_clean = strip_ansi(out)
        try:
            offers = json.loads(out_clean)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"JSON de ofertas inválido: {e}\n{out_clean[:400]}")

        if not offers:
            raise RuntimeError(
                "Sin ofertas. Afloja el filtro DEFAULT_GPU_QUERY "
                "(quita reliability o baja inet_down)."
            )

        # Preferir buena velocidad de red entre las baratas
        def score(o):
            price = float(o.get("dph_total") or 999)
            inet = float(o.get("inet_down") or 0)
            # penaliza caro, bonifica bandwidth
            return price - (inet / 5000.0)

        offer = sorted(offers, key=score)[0]
        offer_id = str(offer.get("id") or offer.get("ask_id")).strip()
        print(
            f"[vast] oferta {offer_id} | {offer.get('gpu_name')} | "
            f"${offer.get('dph_total')}/hr | down={offer.get('inet_down')}Mbps"
        )

        # onstart: bajar wordlist en background apenas el container arranca
        # (corre en paralelo mientras nosotros esperamos SSH)
        # Usar curl/wget que suelen estar; no apt.
        onstart = (
            f"nohup bash -c '"
            f"curl -fsSL -o {remote_path} {shlex.quote(wordlist_url)} "
            f"|| wget -q -O {remote_path} {shlex.quote(wordlist_url)}; "
            f"if file {remote_path} 2>/dev/null | grep -qi gzip; then "
            f"mv {remote_path} {remote_path}.gz && gunzip -f {remote_path}.gz; "
            f"fi; "
            f"touch /root/wordlist.ready"
            f"' >/root/onstart.log 2>&1 &"
        )

        create_cmd = (
            f"vastai create instance {offer_id} "
            f"--image {HASHCAT_IMAGE} "
            f"--disk {DISK_GB} "
            f"--ssh --direct "
            f"--onstart-cmd {shlex.quote(onstart)}"
        )
        rc, out, err = run_cmd(create_cmd, timeout=60)
        if rc != 0:
            raise RuntimeError(f"Falló create instance: {strip_ansi(err or out)}")

        out_clean = strip_ansi(out)
        instance_id = None
        for pattern in [
            r'"new_contract"\s*:\s*(\d+)',
            r"'new_contract'\s*:\s*(\d+)",
            r'"id"\s*:\s*(\d+)',
            r"'id'\s*:\s*(\d+)",
        ]:
            m = re.search(pattern, out_clean)
            if m:
                instance_id = m.group(1)
                break

        if not instance_id:
            time.sleep(3)
            rc2, out2, _ = run_cmd("vastai show instances --raw", timeout=20)
            try:
                instances = json.loads(strip_ansi(out2))
                if instances:
                    instances = sorted(
                        instances, key=lambda x: x.get("start_date") or 0, reverse=True
                    )
                    instance_id = str(instances[0].get("id"))
            except Exception:
                pass

        if not instance_id:
            raise RuntimeError(f"Sin instance_id. Output:\n{out_clean[:600]}")

        # Poll inmediato por SSH info (sin sleep 20s)
        host, port = self._get_ssh_info_retry(instance_id, max_wait=25)
        return instance_id, host, port

    def _get_ssh_info_retry(self, instance_id: str, max_wait: int = 25) -> Tuple[str, int]:
        start = time.time()
        last_err = ""
        while time.time() - start < max_wait:
            try:
                return self._get_ssh_info(instance_id)
            except Exception as e:
                last_err = str(e)
                time.sleep(2)
        raise RuntimeError(f"No pude obtener SSH info a tiempo: {last_err}")

    def _get_ssh_info(self, instance_id: str) -> Tuple[str, int]:
        rc, out, err = run_cmd(f"vastai ssh-url {instance_id}", timeout=15)
        out = strip_ansi(out).strip()
        err = strip_ansi(err).strip()

        if rc == 0 and out:
            if "ssh://" in out:
                part = out.split("ssh://")[-1]
                user_host, port_s = part.rsplit(":", 1)
                host = user_host.split("@")[-1]
                port = int(re.sub(r"[^0-9]", "", port_s))
                return host, port
            m = re.search(r"(\d+\.\d+\.\d+\.\d+)", out)
            mport = re.search(r"(?:-p\s+|:)(\d{2,5})", out)
            if m and mport:
                return m.group(1), int(mport.group(1))

        rc2, out2, _ = run_cmd("vastai show instances --raw", timeout=20)
        out2 = strip_ansi(out2)
        instances = json.loads(out2)
        for inst in instances:
            if str(inst.get("id")) == str(instance_id):
                host = inst.get("public_ipaddr") or inst.get("ssh_host")
                port = inst.get("ssh_port")
                if not port and isinstance(inst.get("ports"), dict):
                    for k, v in inst["ports"].items():
                        if "22" in str(k):
                            port = v.get("HostPort") if isinstance(v, dict) else v
                            break
                if host and port:
                    return str(host), int(port)
        raise RuntimeError(f"ssh-url falló: {out or err}")

    def _instance_status(self, instance_id: str) -> str:
        """Devuelve actual_status / cur_state si se puede (loading, running, etc)."""
        rc, out, _ = run_cmd(f"vastai show instance {instance_id} --raw", timeout=15)
        if rc != 0:
            return "unknown"
        try:
            data = json.loads(strip_ansi(out))
            # a veces viene lista de 1, a veces dict
            if isinstance(data, list) and data:
                data = data[0]
            return str(
                data.get("actual_status")
                or data.get("cur_state")
                or data.get("status_msg")
                or "unknown"
            ).lower()
        except Exception:
            return "unknown"

    def _wait_instance_ready(self, instance_id: str, host: str, port: int, max_wait: int = 150):
        """
        1) Espera a que Vast marque la instancia como running (no solo 'loading')
        2) Después prueba SSH (sirve para proxy sshX.vast.ai y para IP directa)
        """
        start = time.time()
        ssh_opts = (
            f'-i "{self.ssh_key_path}" '
            f"-o StrictHostKeyChecking=no "
            f"-o UserKnownHostsFile=/dev/null "
            f"-o ConnectTimeout=5 "
            f"-o BatchMode=yes "
            f"-o IdentitiesOnly=yes"
        )

        while time.time() - start < max_wait:
            elapsed = int(time.time() - start)
            status = self._instance_status(instance_id)
            print(f"[vast] status={status} | intentando SSH {host}:{port} | t={elapsed}s")

            # Si aún está bajando la imagen, no spamear SSH cada 1s
            if status in ("loading", "created", "scheduling"):
                time.sleep(5)
                continue

            rc, out, err = run_cmd(
                f"ssh {ssh_opts} -p {port} root@{host} \"echo ok\"",
                timeout=12,
            )
            if rc == 0 and "ok" in (out or ""):
                return

            # Reintentar obtener host/port por si cambió de proxy a direct
            try:
                new_host, new_port = self._get_ssh_info(instance_id)
                if new_host != host or new_port != port:
                    print(f"[vast] SSH endpoint actualizado: {new_host}:{new_port}")
                    host, port = new_host, new_port
            except Exception:
                pass

            time.sleep(SSH_POLL_INTERVAL)

        # Debug final
        status = self._instance_status(instance_id)
        rc, out, err = run_cmd(
            f"ssh {ssh_opts} -p {port} root@{host} \"echo ok\"",
            timeout=12,
        )
        raise RuntimeError(
            f"Timeout {max_wait}s esperando SSH en {host}:{port}\n"
            f"status Vast: {status}\n"
            f"ssh rc={rc} out={strip_ansi(out)[:200]} err={strip_ansi(err)[:300]}\n"
            f"Tip: revisa que tu SSH key esté en https://cloud.vast.ai/manage-keys/ "
            f"y que la instancia no esté stuck en loading."
        )

    def _wait_wordlist(self, host: str, port: int, remote_path: str, max_wait: int = 45):
        """Espera a que onstart termine de bajar la wordlist."""
        start = time.time()
        # Rutas posibles según si venía gzip o no
        candidates = [remote_path, remote_path.replace(".gz", ""), "/root/wordlist.txt"]
        while time.time() - start < max_wait:
            check = " || ".join(f"test -s {p}" for p in candidates)
            out = self._ssh(
                host, port,
                f"({check}) && echo READY || (test -f /root/wordlist.ready && echo READY || echo WAIT)",
                timeout=10,
            )
            if "READY" in out:
                # Resolver path final real
                for p in candidates:
                    sz = self._ssh(host, port, f"test -s {p} && echo {p}", timeout=8)
                    if p in sz:
                        return p
                return remote_path
            time.sleep(2)
        # Último intento: si hay algo en /root, seguir igual
        listing = self._ssh(host, port, "ls -la /root/ | head -20", timeout=10)
        raise RuntimeError(
            f"Wordlist no lista en {max_wait}s.\n/root:\n{listing}\n"
            f"Revisa onstart.log en la instancia."
        )

    def _ssh(self, host: str, port: int, remote_cmd: str, timeout: int = 60) -> str:
        full = (
            f'ssh -i "{self.ssh_key_path}" -o StrictHostKeyChecking=no '
            f'-o ConnectTimeout=5 -o BatchMode=yes '
            f'-p {port} root@{host} {shlex.quote(remote_cmd)}'
        )
        rc, out, err = run_cmd(full, timeout=timeout)
        return out

    def _scp_to(self, host: str, port: int, local_path: str, remote_path: str):
        cmd = (
            f'scp -i "{self.ssh_key_path}" -o StrictHostKeyChecking=no '
            f'-o ConnectTimeout=5 -P {port} '
            f'"{local_path}" root@{host}:{remote_path}'
        )
        rc, out, err = run_cmd(cmd, timeout=30)
        if rc != 0:
            raise RuntimeError(f"SCP falló: {strip_ansi(err or out)}")

    def _finish_and_release(
        self, job_id: str, instance_id: str, host: str, port: int, wordlist_path: str, destroy: bool = False
    ):
        """Al terminar el job: o destruye o deja la instancia warm."""
        if destroy or not REUSE_INSTANCES:
            self._destroy_instance(instance_id)
            self._remove_warm_sync(instance_id)
            print(f"[vast] instancia {instance_id} destruida")
        else:
            # Matar hashcat residual y soltar al pool
            self._ssh(host, port, "pkill -9 hashcat 2>/dev/null || true", timeout=10)
            self._release_warm_sync(instance_id, host, port, wordlist_path)

    def _monitor(
        self, job_id: str, instance_id: str, host: str, port: int, wordlist_path: str, max_minutes: int
    ):
        deadline = time.time() + max_minutes * 60
        started = time.time()
        while time.time() < deadline:
            conn = sqlite3.connect(self.jm.db_path)
            row = conn.execute(
                "SELECT status FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            conn.close()
            status = row[0] if row else None
            if status == JobStatus.CANCELLED.value:
                self._update_sync(job_id, finished_at=time.time(), progress="Cancelado")
                # Cancel = destruir (usuario quiere cortar costos)
                self._finish_and_release(job_id, instance_id, host, port, wordlist_path, destroy=True)
                return

            out = self._ssh(
                host, port,
                f"test -f /root/{job_id}.cracked && cat /root/{job_id}.cracked || echo ''",
                timeout=15,
            )
            if out.strip():
                result = out.strip()[:2000]
                # Parsear password si viene hash:password
                plain = result
                if ":" in result:
                    plain = result.rsplit(":", 1)[-1].strip()
                self._update_sync(
                    job_id,
                    status=JobStatus.CRACKED.value,
                    result=result,
                    progress=f"✅ CRACKEADO: `{plain}`",
                    finished_at=time.time(),
                )
                self._finish_and_release(job_id, instance_id, host, port, wordlist_path, destroy=False)
                return

            # Progress desde log de hashcat
            log_tail = self._ssh(
                host, port,
                f"tail -5 /root/{job_id}.log 2>/dev/null || true",
                timeout=10,
            )
            progress_line = "Hashcat corriendo..."
            for line in reversed((log_tail or "").splitlines()):
                line = line.strip()
                if not line:
                    continue
                # Status típico de hashcat
                if "Speed" in line or "Progress" in line or "Status" in line or "%" in line:
                    progress_line = line[:180]
                    break
                progress_line = line[:180]

            elapsed = int(time.time() - started)
            self._update_sync(
                job_id,
                progress=f"[{elapsed}s] {progress_line}",
            )

            ps = self._ssh(host, port, "pgrep -a hashcat || true", timeout=10)
            if "hashcat" not in ps and (time.time() - started) > 60:
                log = self._ssh(host, port, f"tail -40 /root/{job_id}.log 2>/dev/null || true", timeout=10)
                self._update_sync(
                    job_id,
                    status=JobStatus.FAILED.value,
                    error=f"Hashcat terminó sin crackear.\n{(log or '')[:400]}",
                    progress="Sin resultado (exhausted / error)",
                    finished_at=time.time(),
                )
                # Instancia sigue usable
                self._finish_and_release(job_id, instance_id, host, port, wordlist_path, destroy=False)
                return

            time.sleep(5)

        self._update_sync(
            job_id,
            status=JobStatus.TIMEOUT.value,
            error=f"Timeout después de {max_minutes} min",
            progress="Timeout",
            finished_at=time.time(),
        )
        self._finish_and_release(job_id, instance_id, host, port, wordlist_path, destroy=False)

    def _destroy_instance(self, instance_id: str):
        run_cmd(f"vastai destroy instance {instance_id}", timeout=45)

    async def cancel_job(self, job_id: str) -> bool:
        job = await self.jm.get_job(job_id)
        if not job:
            return False
        if job["status"] in (
            JobStatus.CRACKED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        ):
            return False
        await self.jm.update_job(
            job_id,
            status=JobStatus.CANCELLED.value,
            progress="Cancelado",
            finished_at=time.time(),
        )
        # Cancel = cortar costos: destruir instancia y sacarla del pool
        if job.get("instance_id"):
            iid = job["instance_id"]
            await asyncio.to_thread(self._destroy_instance, iid)
            await self.jm.remove_warm_instance(iid)
        return True

    async def destroy_warm_instance(self, instance_id: str) -> bool:
        await self.jm.remove_warm_instance(instance_id)
        await asyncio.to_thread(self._destroy_instance, instance_id)
        return True

    async def cleanup_idle_instances(self) -> int:
        """Destruye warms idle hace más de INSTANCE_IDLE_MINUTES."""
        idle = await self.jm.get_idle_warm_instances(INSTANCE_IDLE_MINUTES * 60)
        count = 0
        for inst in idle:
            try:
                await self.destroy_warm_instance(inst["instance_id"])
                count += 1
                print(f"[vast] idle cleanup: destruida {inst['instance_id']}")
            except Exception as e:
                print(f"[vast] cleanup error {inst['instance_id']}: {e}")
        return count
