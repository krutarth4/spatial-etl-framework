"""
experimentation/run_experiment.py
──────────────────────────────────
Patches config.yaml so that every enabled datasource fires
immediately through APScheduler (instead of waiting hours/days),
then runs the normal pipeline (same as python3 run.py).

How it works:
  1. Reads config.yaml via YamlReader (resolves ${{}} env-blocks).
  2. Enables the APScheduler.
  3. Changes every enabled datasource trigger to 'run_once'
     (APScheduler DateTrigger set to datetime.now()).
  4. Writes the patched config to experimentation/.experiment_config.yaml.
  5. Points CoreConfig at that temp file and calls Application().run_standalone().

Re-run any time — each run re-generates the patched config so all
enabled jobs fire in the next second.

Usage (from project root):
    python3 experimentation/run_experiment.py
"""

from __future__ import annotations

import sys
import yaml
from pathlib import Path

# ── project root on path ──────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env")

from banner import print_banner, log_banner
print_banner()

# ── paths ─────────────────────────────────────────────────────────────────────
_CONFIG_SRC  = _ROOT / "config.yaml"
_CONFIG_DEST = _ROOT / ".experiment_config.yaml"

# ── Step 1: read and resolve the real config ──────────────────────────────────
from readers.yaml_reader import YamlReader

print(f"[experiment] Reading config: {_CONFIG_SRC}")
config = YamlReader(str(_CONFIG_SRC)).read()

# ── Step 2: enable the scheduler ─────────────────────────────────────────────
config["scheduler"]["enable"] = True
print("[experiment] Scheduler → enabled")

# ── Step 3: patch every enabled datasource trigger → run_once ─────────────────
# 'run_once' maps to DateTrigger(run_date=datetime.now()) inside create_job().
# We only touch enabled datasources; disabled ones are left alone.
patched: list[str] = []
for ds in config.get("datasources") or []:
    if not isinstance(ds, dict) or not ds.get("enable", False):
        continue
    job = ds.get("job")
    if not isinstance(job, dict):
        continue
    trigger = job.get("trigger")
    if not isinstance(trigger, dict):
        continue
    ttype = trigger.get("type") or {}
    old_name = ttype.get("name", "?")
    if old_name != "run_once":
        # Replace the whole trigger.type block; run_once needs no extra fields.
        trigger["type"] = {
            "name":       "run_once",
            "start_date": None,
            "end_date":   None,
            "config":     None,
        }
        patched.append(f"{ds['name']} ({old_name} → run_once)")

if patched:
    print(f"[experiment] Patched triggers: {patched}")
else:
    print("[experiment] All enabled triggers already set to run_once — nothing to patch.")

# ── Step 4: write patched config to temp file ─────────────────────────────────
with open(_CONFIG_DEST, "w") as fh:
    yaml.dump(config, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
print(f"[experiment] Wrote patched config → {_CONFIG_DEST}")

# ── Step 5: point CoreConfig at the temp file, then run the pipeline ──────────
# CoreConfig is a singleton; we set the class-level filepath BEFORE any import
# instantiates it so it picks up our patched file automatically.
import main_core.core_config as _cc
_cc.CoreConfig.filepath = str(_CONFIG_DEST)
print(f"[experiment] CoreConfig.filepath → {_CONFIG_DEST.name}")

from utils.logger_manager import setup_file_logging
setup_file_logging(_cc.CoreConfig().get_config().get("logging") or {})
log_banner()  # plain-text banner → pipeline.log (file handler now attached)

print("[experiment] Starting Application …\n")

from core.application import Application
Application().run_standalone()
