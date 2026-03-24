# config.py
"""
Configuration BACCARAT AI
Toutes les valeurs sont lues depuis les variables d'environnement.
Sur Render.com elles sont injectées automatiquement.
"""

import os

# ── Telegram ──────────────────────────────────────────────────────────────────
ADMIN_ID         = int(os.getenv('ADMIN_ID', '0'))
API_ID           = int(os.getenv('API_ID',   '0'))
API_HASH         = os.getenv('API_HASH',     '')
BOT_TOKEN        = os.getenv('BOT_TOKEN',    '')
TELEGRAM_SESSION = os.getenv('TELEGRAM_SESSION', '')

# Admins supplémentaires (IDs fixes + variable d'env optionnelle)
_extra_admins_env = os.getenv('EXTRA_ADMIN_IDS', '')
_extra_admins = [int(x.strip()) for x in _extra_admins_env.split(',') if x.strip().isdigit()]
EXTRA_ADMIN_IDS: list = [1190237801, 1309049556] + _extra_admins

def is_admin(user_id: int) -> bool:
    """Retourne True si l'utilisateur est admin principal ou admin supplémentaire."""
    if ADMIN_ID != 0 and user_id == ADMIN_ID:
        return True
    return user_id in EXTRA_ADMIN_IDS

# ── Base de données ───────────────────────────────────────────────────────────
# Sur Render.com : définir RENDER_DB_URL dans les variables d'environnement
RENDER_DB_URL = os.getenv('RENDER_DB_URL', '')

# ── Serveur web ───────────────────────────────────────────────────────────────
# Render.com injecte automatiquement PORT=10000
PORT = int(os.getenv('PORT', '10000'))

# ── Capture API ───────────────────────────────────────────────────────────────
API_POLL_INTERVAL = int(os.getenv('API_POLL_INTERVAL', '8'))
