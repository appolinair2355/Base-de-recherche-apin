"""
Gestion de la base de données PostgreSQL Render
Stockage complet des parties Baccarat 1xBet
"""
import os
import logging
from datetime import datetime, timedelta, date as date_cls, time as time_cls
from typing import Optional, List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

_raw_db_url = os.getenv("RENDER_DB_URL", "")
if not _raw_db_url:
    raise RuntimeError("Variable d'environnement RENDER_DB_URL manquante. Configurez-la sur Render.com.")
# Normaliser : s'assurer que c'est une URL postgresql:// complète
if _raw_db_url.startswith("postgresql://") or _raw_db_url.startswith("postgres://"):
    DB_URL = _raw_db_url
else:
    # Format interne Render (ex: dpg-xxx-a/dbname) — on préfixe pour psycopg2
    logger.warning(
        f"RENDER_DB_URL ne commence pas par postgresql:// (reçu: '{_raw_db_url[:40]}'). "
        "Définissez l'URL externe complète dans la variable RENDER_DB_URL."
    )
    DB_URL = "postgresql://" + _raw_db_url

RANK_NAME = {
    1: "As", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
    7: "7", 8: "8", 9: "9", 10: "10",
    11: "Valet", 12: "Dame", 13: "Roi"
}
RANK_SHORT = {
    1: "A", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
    7: "7", 8: "8", 9: "9", 10: "10",
    11: "J", 12: "Q", 13: "K"
}
ENS_NAME = {0: "Pique", 1: "Trefle", 2: "Carreau", 3: "Coeur"}
ENS_EMOJI = {0: "♠", 1: "♣", 2: "♦", 3: "♥"}
ENS_PAIR  = {0: "Pique/Carreau", 2: "Pique/Carreau", 1: "Trefle/Coeur", 3: "Trefle/Coeur"}


def baccarat_value(rank: int) -> int:
    if 1 <= rank <= 9:
        return rank
    return 0


def get_conn():
    if not DB_URL:
        raise RuntimeError("RENDER_DB_URL non configuré")
    return psycopg2.connect(DB_URL, connect_timeout=15)


def init_db():
    """Crée les tables si elles n'existent pas."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS parties (
        id                    SERIAL PRIMARY KEY,
        date_jeu              DATE NOT NULL,
        numero_jeu            INTEGER NOT NULL,
        heure                 TIME,

        joueur_carte1_rang    VARCHAR(5),
        joueur_carte1_ens     VARCHAR(10),
        joueur_carte1_val     INTEGER,
        joueur_carte2_rang    VARCHAR(5),
        joueur_carte2_ens     VARCHAR(10),
        joueur_carte2_val     INTEGER,
        joueur_carte3_rang    VARCHAR(5),
        joueur_carte3_ens     VARCHAR(10),
        joueur_carte3_val     INTEGER,
        joueur_total_cartes   INTEGER,
        joueur_points         INTEGER,

        banquier_carte1_rang  VARCHAR(5),
        banquier_carte1_ens   VARCHAR(10),
        banquier_carte1_val   INTEGER,
        banquier_carte2_rang  VARCHAR(5),
        banquier_carte2_ens   VARCHAR(10),
        banquier_carte2_val   INTEGER,
        banquier_carte3_rang  VARCHAR(5),
        banquier_carte3_ens   VARCHAR(10),
        banquier_carte3_val   INTEGER,
        banquier_total_cartes INTEGER,
        banquier_points       INTEGER,

        gagnant               VARCHAR(10),
        total_cartes          INTEGER,
        est_paire_joueur      BOOLEAN,
        est_paire_banquier    BOOLEAN,
        naturel               BOOLEAN,
        joueur_troisieme      BOOLEAN,
        banquier_troisieme    BOOLEAN,

        enregistre_le         TIMESTAMP DEFAULT NOW(),

        UNIQUE(date_jeu, numero_jeu)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parties_date     ON parties(date_jeu);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parties_num      ON parties(numero_jeu);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parties_gagnant  ON parties(gagnant);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parties_date_num ON parties(date_jeu, numero_jeu);")
    conn.commit()
    conn.close()
    logger.info("DB initialisée")


def _card_info(card: dict) -> dict:
    r = card.get("rank", 0)
    s = card.get("suit", -1)
    return {
        "rang":  RANK_SHORT.get(r, str(r)),
        "ens":   ENS_EMOJI.get(s, "?"),
        "val":   baccarat_value(r),
        "rank":  r,
        "suit":  s,
    }


def save_game(game: dict, date_jeu: date_cls, heure: Optional[time_cls] = None) -> bool:
    """Enregistre une partie terminée dans la base de données."""
    try:
        pc_raw = game.get("player_cards", [])
        bc_raw = game.get("banker_cards", [])
        winner_raw = game.get("winner")

        pc = [_card_info(c) for c in pc_raw]
        bc = [_card_info(c) for c in bc_raw]

        def pts(cards):
            return sum(c["val"] for c in cards) % 10

        def paire(cards):
            if len(cards) >= 2:
                return cards[0]["rank"] == cards[1]["rank"]
            return False

        def naturel(cards):
            if len(cards) == 2:
                return pts(cards) >= 8
            return False

        gagnant_map = {"Player": "Joueur", "Banker": "Banquier", "Tie": "Egalite"}
        gagnant = gagnant_map.get(winner_raw, winner_raw)

        def c(cards, i, field):
            if i < len(cards):
                return cards[i][field]
            return None

        row = {
            "date_jeu":              date_jeu,
            "numero_jeu":            game["game_number"],
            "heure":                 heure,

            "joueur_carte1_rang":    c(pc, 0, "rang"),
            "joueur_carte1_ens":     c(pc, 0, "ens"),
            "joueur_carte1_val":     c(pc, 0, "val"),
            "joueur_carte2_rang":    c(pc, 1, "rang"),
            "joueur_carte2_ens":     c(pc, 1, "ens"),
            "joueur_carte2_val":     c(pc, 1, "val"),
            "joueur_carte3_rang":    c(pc, 2, "rang"),
            "joueur_carte3_ens":     c(pc, 2, "ens"),
            "joueur_carte3_val":     c(pc, 2, "val"),
            "joueur_total_cartes":   len(pc),
            "joueur_points":         pts(pc),

            "banquier_carte1_rang":  c(bc, 0, "rang"),
            "banquier_carte1_ens":   c(bc, 0, "ens"),
            "banquier_carte1_val":   c(bc, 0, "val"),
            "banquier_carte2_rang":  c(bc, 1, "rang"),
            "banquier_carte2_ens":   c(bc, 1, "ens"),
            "banquier_carte2_val":   c(bc, 1, "val"),
            "banquier_carte3_rang":  c(bc, 2, "rang"),
            "banquier_carte3_ens":   c(bc, 2, "ens"),
            "banquier_carte3_val":   c(bc, 2, "val"),
            "banquier_total_cartes": len(bc),
            "banquier_points":       pts(bc),

            "gagnant":               gagnant,
            "total_cartes":          len(pc) + len(bc),
            "est_paire_joueur":      paire(pc),
            "est_paire_banquier":    paire(bc),
            "naturel":               naturel(pc) or naturel(bc),
            "joueur_troisieme":      len(pc) >= 3,
            "banquier_troisieme":    len(bc) >= 3,
        }

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO parties (
                date_jeu, numero_jeu, heure,
                joueur_carte1_rang, joueur_carte1_ens, joueur_carte1_val,
                joueur_carte2_rang, joueur_carte2_ens, joueur_carte2_val,
                joueur_carte3_rang, joueur_carte3_ens, joueur_carte3_val,
                joueur_total_cartes, joueur_points,
                banquier_carte1_rang, banquier_carte1_ens, banquier_carte1_val,
                banquier_carte2_rang, banquier_carte2_ens, banquier_carte2_val,
                banquier_carte3_rang, banquier_carte3_ens, banquier_carte3_val,
                banquier_total_cartes, banquier_points,
                gagnant, total_cartes,
                est_paire_joueur, est_paire_banquier, naturel,
                joueur_troisieme, banquier_troisieme
            ) VALUES (
                %(date_jeu)s, %(numero_jeu)s, %(heure)s,
                %(joueur_carte1_rang)s, %(joueur_carte1_ens)s, %(joueur_carte1_val)s,
                %(joueur_carte2_rang)s, %(joueur_carte2_ens)s, %(joueur_carte2_val)s,
                %(joueur_carte3_rang)s, %(joueur_carte3_ens)s, %(joueur_carte3_val)s,
                %(joueur_total_cartes)s, %(joueur_points)s,
                %(banquier_carte1_rang)s, %(banquier_carte1_ens)s, %(banquier_carte1_val)s,
                %(banquier_carte2_rang)s, %(banquier_carte2_ens)s, %(banquier_carte2_val)s,
                %(banquier_carte3_rang)s, %(banquier_carte3_ens)s, %(banquier_carte3_val)s,
                %(banquier_total_cartes)s, %(banquier_points)s,
                %(gagnant)s, %(total_cartes)s,
                %(est_paire_joueur)s, %(est_paire_banquier)s, %(naturel)s,
                %(joueur_troisieme)s, %(banquier_troisieme)s
            )
            ON CONFLICT (date_jeu, numero_jeu) DO NOTHING;
        """, row)
        inserted = cur.rowcount
        conn.commit()
        conn.close()
        return inserted > 0
    except Exception as e:
        logger.error(f"save_game error jeu#{game.get('game_number','?')}: {e}")
        return False


def get_games_by_date(target_date: date_cls) -> List[Dict]:
    """Récupère toutes les parties d'une date depuis la DB."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM parties
        WHERE date_jeu = %s
        ORDER BY numero_jeu ASC
    """, (target_date,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_dates_available() -> List[date_cls]:
    """Retourne la liste des dates qui ont des données en DB."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date_jeu FROM parties ORDER BY date_jeu DESC LIMIT 30;")
    dates = [r[0] for r in cur.fetchall()]
    conn.close()
    return dates


def get_stats_by_date(target_date: date_cls) -> Dict:
    """Calcule les statistiques complètes pour une date."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*)                                          AS total,
            COUNT(*) FILTER (WHERE gagnant='Joueur')         AS joueur_wins,
            COUNT(*) FILTER (WHERE gagnant='Banquier')       AS banquier_wins,
            COUNT(*) FILTER (WHERE gagnant='Egalite')        AS egalites,
            COUNT(*) FILTER (WHERE naturel=true)             AS naturels,
            COUNT(*) FILTER (WHERE est_paire_joueur=true)    AS paires_joueur,
            COUNT(*) FILTER (WHERE est_paire_banquier=true)  AS paires_banquier,
            COUNT(*) FILTER (WHERE joueur_troisieme=true)    AS tirage_3j,
            COUNT(*) FILTER (WHERE banquier_troisieme=true)  AS tirage_3b,
            COUNT(*) FILTER (WHERE total_cartes=4)           AS jeux_4cartes,
            COUNT(*) FILTER (WHERE total_cartes=5)           AS jeux_5cartes,
            COUNT(*) FILTER (WHERE total_cartes=6)           AS jeux_6cartes,
            MIN(numero_jeu)                                   AS premier_jeu,
            MAX(numero_jeu)                                   AS dernier_jeu
        FROM parties
        WHERE date_jeu = %s
    """, (target_date,))
    row = dict(cur.fetchone())
    conn.close()
    return row


def count_games_for_date(target_date: date_cls) -> int:
    """Compte le nombre de parties enregistrées pour une date."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM parties WHERE date_jeu = %s", (target_date,))
    count = cur.fetchone()[0]
    conn.close()
    return count


def get_last_saved_game_num(target_date: date_cls) -> int:
    """Retourne le numéro du dernier jeu enregistré pour une date."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(numero_jeu) FROM parties WHERE date_jeu = %s", (target_date,))
    result = cur.fetchone()[0]
    conn.close()
    return result or 0


def get_comptage_today(today: date_cls, current_game_num: int) -> dict:
    """Statistiques de couverture pour aujourd'hui du jeu #1 au jeu actuel."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*)                                         AS enregistres,
            COUNT(*) FILTER (WHERE gagnant='Joueur')         AS joueur_wins,
            COUNT(*) FILTER (WHERE gagnant='Banquier')       AS banquier_wins,
            COUNT(*) FILTER (WHERE gagnant='Egalite')        AS egalites,
            MIN(numero_jeu)                                   AS premier,
            MAX(numero_jeu)                                   AS dernier,
            COUNT(*) FILTER (WHERE numero_jeu <= %s)         AS dans_plage
        FROM parties
        WHERE date_jeu = %s
    """, (current_game_num, today))
    row = dict(cur.fetchone())

    # Jeux manquants dans la plage 1 -> current_game_num
    if current_game_num > 0:
        cur.execute("""
            SELECT COUNT(*) AS manquants
            FROM generate_series(1, %s) AS s(n)
            WHERE s.n NOT IN (
                SELECT numero_jeu FROM parties WHERE date_jeu = %s
            )
        """, (current_game_num, today))
        row["manquants"] = (cur.fetchone() or {}).get("manquants", 0)
    else:
        row["manquants"] = 0

    conn.close()
    return row


def get_global_total() -> dict:
    """Total global de toutes les parties depuis la création de la base."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            COUNT(*)                                   AS total_global,
            COUNT(DISTINCT date_jeu)                   AS nb_jours,
            MIN(date_jeu)                              AS premier_jour,
            MAX(date_jeu)                              AS dernier_jour,
            COUNT(*) FILTER (WHERE gagnant='Joueur')   AS total_joueur,
            COUNT(*) FILTER (WHERE gagnant='Banquier') AS total_banquier,
            COUNT(*) FILTER (WHERE gagnant='Egalite')  AS total_egalite,
            COUNT(*) FILTER (WHERE naturel=true)        AS total_naturels,
            COUNT(*) FILTER (WHERE total_cartes=4)      AS jeux_4c,
            COUNT(*) FILTER (WHERE total_cartes=5)      AS jeux_5c,
            COUNT(*) FILTER (WHERE total_cartes=6)      AS jeux_6c
        FROM parties
    """)
    row = dict(cur.fetchone())

    # Détail par jour
    cur.execute("""
        SELECT date_jeu, COUNT(*) AS nb,
               COUNT(*) FILTER (WHERE gagnant='Joueur')   AS j,
               COUNT(*) FILTER (WHERE gagnant='Banquier') AS b,
               COUNT(*) FILTER (WHERE gagnant='Egalite')  AS e,
               MIN(numero_jeu) AS min_jeu, MAX(numero_jeu) AS max_jeu
        FROM parties
        GROUP BY date_jeu
        ORDER BY date_jeu DESC
        LIMIT 14
    """)
    row["detail_jours"] = [dict(r) for r in cur.fetchall()]
    conn.close()
    return row


def _suit_cond(side: str, suit: str) -> str:
    """Condition SQL : au moins une carte du côté 'side' a la couleur 'suit'."""
    c1, c2, c3 = (f"{side}_carte{i}_ens" for i in (1, 2, 3))
    return f"({c1}='{suit}' OR {c2}='{suit}' OR {c3}='{suit}')"

def _rang_cond(side: str, rang: str) -> str:
    """Condition SQL : au moins une carte du côté 'side' a le rang 'rang'."""
    c1, c2, c3 = (f"{side}_carte{i}_rang" for i in (1, 2, 3))
    return f"({c1}='{rang}' OR {c2}='{rang}' OR {c3}='{rang}')"

FILTRES_DISPONIBLES = {
    # ── Configuration de cartes ───────────────────────────────────────────────
    "2/2": ("joueur_total_cartes = 2", "banquier_total_cartes = 2"),
    "3/2": ("joueur_total_cartes = 3", "banquier_total_cartes = 2"),
    "2/3": ("joueur_total_cartes = 2", "banquier_total_cartes = 3"),
    "3/3": ("joueur_total_cartes = 3", "banquier_total_cartes = 3"),
    # ── Résultat ─────────────────────────────────────────────────────────────
    "joueur":         ("gagnant = 'Joueur'",),
    "banquier":       ("gagnant = 'Banquier'",),
    "egalite":        ("gagnant = 'Egalite'",),
    # ── Événements ───────────────────────────────────────────────────────────
    "naturel":        ("naturel = true",),
    "paire_joueur":   ("est_paire_joueur = true",),
    "paire_banquier": ("est_paire_banquier = true",),
    # ── Seuils points Joueur ─────────────────────────────────────────────────
    "plus65_joueur":  ("joueur_points >= 7",),
    "moins45_joueur": ("joueur_points <= 4",),
    # ── Seuils points Banquier ───────────────────────────────────────────────
    "plus65_banquier":  ("banquier_points >= 7",),
    "moins45_banquier": ("banquier_points <= 4",),
    # ── Couleurs côté Joueur (au moins 1 carte de cette couleur) ─────────────
    "pique_joueur":   (_suit_cond("joueur", "♠"),),
    "trefle_joueur":  (_suit_cond("joueur", "♣"),),
    "carreau_joueur": (_suit_cond("joueur", "♦"),),
    "coeur_joueur":   (_suit_cond("joueur", "♥"),),
    # ── Couleurs côté Banquier ───────────────────────────────────────────────
    "pique_banquier":   (_suit_cond("banquier", "♠"),),
    "trefle_banquier":  (_suit_cond("banquier", "♣"),),
    "carreau_banquier": (_suit_cond("banquier", "♦"),),
    "coeur_banquier":   (_suit_cond("banquier", "♥"),),
    # ── Cartes hautes côté Joueur ────────────────────────────────────────────
    "as_joueur":    (_rang_cond("joueur", "A"),),
    "roi_joueur":   (_rang_cond("joueur", "K"),),
    "dame_joueur":  (_rang_cond("joueur", "Q"),),
    "valet_joueur": (_rang_cond("joueur", "J"),),
    "dix_joueur":   (_rang_cond("joueur", "10"),),
    # ── Cartes hautes côté Banquier ──────────────────────────────────────────
    "as_banquier":    (_rang_cond("banquier", "A"),),
    "roi_banquier":   (_rang_cond("banquier", "K"),),
    "dame_banquier":  (_rang_cond("banquier", "Q"),),
    "valet_banquier": (_rang_cond("banquier", "J"),),
    "dix_banquier":   (_rang_cond("banquier", "10"),),
}


def search_games_by_filter(target_date: date_cls, filtre: str) -> List[Dict]:
    """Recherche les parties d'une date selon un filtre catalogue.
    filtres acceptés : '2/2', '3/2', '2/3', '3/3',
                       'joueur', 'banquier', 'egalite',
                       'naturel', 'paire_joueur', 'paire_banquier'
    """
    conds = FILTRES_DISPONIBLES.get(filtre.lower().strip(), ())
    if not conds:
        return []
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    where = " AND ".join(["date_jeu = %s"] + list(conds))
    cur.execute(f"SELECT * FROM parties WHERE {where} ORDER BY numero_jeu", [target_date])
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def row_matches_filter(row: dict, filtre: str) -> bool:
    """Vérifie localement si une ligne correspond à un filtre donné."""
    f = filtre.lower().strip()
    # Config cartes
    if f == "2/2": return row.get("joueur_total_cartes")==2 and row.get("banquier_total_cartes")==2
    if f == "3/2": return row.get("joueur_total_cartes")==3 and row.get("banquier_total_cartes")==2
    if f == "2/3": return row.get("joueur_total_cartes")==2 and row.get("banquier_total_cartes")==3
    if f == "3/3": return row.get("joueur_total_cartes")==3 and row.get("banquier_total_cartes")==3
    # Résultat
    if f == "joueur":   return row.get("gagnant") == "Joueur"
    if f == "banquier": return row.get("gagnant") == "Banquier"
    if f == "egalite":  return row.get("gagnant") == "Egalite"
    # Événements
    if f == "naturel":        return bool(row.get("naturel"))
    if f == "paire_joueur":   return bool(row.get("est_paire_joueur"))
    if f == "paire_banquier": return bool(row.get("est_paire_banquier"))
    # Seuils
    pj = int(row.get("joueur_points",  0) or 0)
    pb = int(row.get("banquier_points", 0) or 0)
    if f == "plus65_joueur":   return pj >= 7
    if f == "moins45_joueur":  return pj <= 4
    if f == "plus65_banquier": return pb >= 7
    if f == "moins45_banquier":return pb <= 4
    # Couleurs
    SUIT = {"pique": "♠", "trefle": "♣", "carreau": "♦", "coeur": "♥"}
    for sn, ss in SUIT.items():
        if f == f"{sn}_joueur":   return any(row.get(f"joueur_carte{i}_ens")==ss   for i in (1,2,3))
        if f == f"{sn}_banquier": return any(row.get(f"banquier_carte{i}_ens")==ss for i in (1,2,3))
    # Rangs
    RANK = {"as":"A","roi":"K","dame":"Q","valet":"J","dix":"10"}
    for rn, rv in RANK.items():
        if f == f"{rn}_joueur":   return any(row.get(f"joueur_carte{i}_rang")==rv   for i in (1,2,3))
        if f == f"{rn}_banquier": return any(row.get(f"banquier_carte{i}_rang")==rv for i in (1,2,3))
    return False


def search_games_multi_filter(target_date: date_cls, filtres: List[str]) -> List[Dict]:
    """Recherche multi-filtre (OR) sur une date.
    Chaque ligne retournée contient 'matched_filters' = liste des filtres correspondants.
    """
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM parties WHERE date_jeu = %s ORDER BY numero_jeu", [target_date])
    all_rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    result = []
    for row in all_rows:
        matched = [f for f in filtres if row_matches_filter(row, f)]
        if matched:
            row["matched_filters"] = matched
            result.append(row)
    return result


def compare_dates(date_a: date_cls, date_b: date_cls) -> List[Dict]:
    """Compare deux jours et retourne les parties communes (même numéro_jeu).
    Pour chaque numéro commun, calcule les correspondances entre les catégories.
    """
    games_a = {r["numero_jeu"]: r for r in get_games_by_date(date_a)}
    games_b = {r["numero_jeu"]: r for r in get_games_by_date(date_b)}
    common  = sorted(set(games_a) & set(games_b))

    def cfg(r):
        return f"{r.get('joueur_total_cartes',2)}/{r.get('banquier_total_cartes',2)}"

    def has_suit(r, side, suit):
        return any(r.get(f"{side}_carte{i}_ens") == suit for i in (1, 2, 3))

    def has_rang(r, side, rang):
        return any(r.get(f"{side}_carte{i}_rang") == rang for i in (1, 2, 3))

    result = []
    for num in common:
        a, b = games_a[num], games_b[num]
        pj_a = int(a.get("joueur_points",  0) or 0)
        pb_a = int(a.get("banquier_points", 0) or 0)
        pj_b = int(b.get("joueur_points",  0) or 0)
        pb_b = int(b.get("banquier_points", 0) or 0)

        row = {
            "numero_jeu": num,
            # ── Jour A ───────────────────────────────────────────────────────
            "a_date":    a.get("date_jeu"),
            "a_heure":   str(a.get("heure", ""))[:5],
            "a_config":  cfg(a),
            "a_gagnant": a.get("gagnant", "?"),
            "a_pts_j":   pj_a,
            "a_pts_b":   pb_a,
            "a_sup65_j": pj_a >= 7,
            "a_inf45_j": pj_a <= 4,
            "a_sup65_b": pb_a >= 7,
            "a_inf45_b": pb_a <= 4,
            "a_naturel": bool(a.get("naturel")),
            "a_paire_j": bool(a.get("est_paire_joueur")),
            "a_paire_b": bool(a.get("est_paire_banquier")),
            # Couleurs Joueur A
            "a_pique_j":   has_suit(a, "joueur", "♠"),
            "a_trefle_j":  has_suit(a, "joueur", "♣"),
            "a_carreau_j": has_suit(a, "joueur", "♦"),
            "a_coeur_j":   has_suit(a, "joueur", "♥"),
            # Couleurs Banquier A
            "a_pique_b":   has_suit(a, "banquier", "♠"),
            "a_trefle_b":  has_suit(a, "banquier", "♣"),
            "a_carreau_b": has_suit(a, "banquier", "♦"),
            "a_coeur_b":   has_suit(a, "banquier", "♥"),
            # ── Jour B ───────────────────────────────────────────────────────
            "b_date":    b.get("date_jeu"),
            "b_heure":   str(b.get("heure", ""))[:5],
            "b_config":  cfg(b),
            "b_gagnant": b.get("gagnant", "?"),
            "b_pts_j":   pj_b,
            "b_pts_b":   pb_b,
            "b_sup65_j": pj_b >= 7,
            "b_inf45_j": pj_b <= 4,
            "b_sup65_b": pb_b >= 7,
            "b_inf45_b": pb_b <= 4,
            "b_naturel": bool(b.get("naturel")),
            "b_paire_j": bool(b.get("est_paire_joueur")),
            "b_paire_b": bool(b.get("est_paire_banquier")),
            # Couleurs Joueur B
            "b_pique_j":   has_suit(b, "joueur", "♠"),
            "b_trefle_j":  has_suit(b, "joueur", "♣"),
            "b_carreau_j": has_suit(b, "joueur", "♦"),
            "b_coeur_j":   has_suit(b, "joueur", "♥"),
            # Couleurs Banquier B
            "b_pique_b":   has_suit(b, "banquier", "♠"),
            "b_trefle_b":  has_suit(b, "banquier", "♣"),
            "b_carreau_b": has_suit(b, "banquier", "♦"),
            "b_coeur_b":   has_suit(b, "banquier", "♥"),
        }
        # ── Correspondances (identique sur les deux jours) ───────────────────
        row["match_gagnant"]  = a.get("gagnant") == b.get("gagnant")
        row["match_config"]   = cfg(a) == cfg(b)
        row["match_sup65_j"]  = row["a_sup65_j"]  == row["b_sup65_j"]
        row["match_inf45_j"]  = row["a_inf45_j"]  == row["b_inf45_j"]
        row["match_sup65_b"]  = row["a_sup65_b"]  == row["b_sup65_b"]
        row["match_inf45_b"]  = row["a_inf45_b"]  == row["b_inf45_b"]
        row["match_naturel"]  = row["a_naturel"]  == row["b_naturel"]
        row["match_paire_j"]  = row["a_paire_j"]  == row["b_paire_j"]
        row["match_paire_b"]  = row["a_paire_b"]  == row["b_paire_b"]
        row["match_pique_j"]  = row["a_pique_j"]  == row["b_pique_j"]
        row["match_trefle_j"] = row["a_trefle_j"] == row["b_trefle_j"]
        row["match_carreau_j"]= row["a_carreau_j"]== row["b_carreau_j"]
        row["match_coeur_j"]  = row["a_coeur_j"]  == row["b_coeur_j"]
        row["match_pique_b"]  = row["a_pique_b"]  == row["b_pique_b"]
        row["match_trefle_b"] = row["a_trefle_b"] == row["b_trefle_b"]
        row["match_carreau_b"]= row["a_carreau_b"]== row["b_carreau_b"]
        row["match_coeur_b"]  = row["a_coeur_b"]  == row["b_coeur_b"]
        # Nombre de correspondances exactes
        row["nb_matchs"] = sum(1 for k, v in row.items() if k.startswith("match_") and v)
        result.append(row)
    return result


def stats_from_rows(rows: List[Dict]) -> Dict:
    """Calcule les statistiques à partir d'une liste de lignes (filtrage local)."""
    total  = len(rows)
    jw     = sum(1 for r in rows if r.get("gagnant") == "Joueur")
    bw     = sum(1 for r in rows if r.get("gagnant") == "Banquier")
    eg     = sum(1 for r in rows if r.get("gagnant") == "Egalite")
    nat    = sum(1 for r in rows if r.get("naturel"))
    pj     = sum(1 for r in rows if r.get("est_paire_joueur"))
    pb     = sum(1 for r in rows if r.get("est_paire_banquier"))
    j4     = sum(1 for r in rows if int(r.get("total_cartes") or 0) == 4)
    j5     = sum(1 for r in rows if int(r.get("total_cartes") or 0) == 5)
    j6     = sum(1 for r in rows if int(r.get("total_cartes") or 0) == 6)
    nums   = [int(r.get("numero_jeu") or 0) for r in rows]
    return {
        "total": total, "joueur_wins": jw, "banquier_wins": bw,
        "egalites": eg, "naturels": nat,
        "paires_joueur": pj, "paires_banquier": pb,
        "jeux_4cartes": j4, "jeux_5cartes": j5, "jeux_6cartes": j6,
        "premier_jeu": min(nums) if nums else 0,
        "dernier_jeu":  max(nums) if nums else 0,
    }


def get_db_size() -> dict:
    """Retourne la taille de la base de données et des tables."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            pg_size_pretty(pg_database_size(current_database()))   AS taille_totale_db,
            pg_size_pretty(pg_total_relation_size('parties'))       AS taille_table_parties,
            pg_size_pretty(pg_indexes_size('parties'))              AS taille_index_parties,
            pg_size_pretty(pg_relation_size('parties'))             AS taille_donnees_parties,
            pg_database_size(current_database())                    AS octets_db,
            pg_total_relation_size('parties')                       AS octets_parties,
            current_database()                                      AS nom_base
    """)
    row = dict(cur.fetchone())
    cur.execute("SELECT COUNT(*) AS total, COUNT(DISTINCT date_jeu) AS jours FROM parties")
    stats = dict(cur.fetchone())
    row.update(stats)
    cur.execute("""
        SELECT
            reltuples::BIGINT  AS estimation_lignes,
            relpages           AS nb_pages
        FROM pg_class WHERE relname = 'parties'
    """)
    pg = cur.fetchone()
    row["estimation_lignes"] = int(pg["estimation_lignes"]) if pg else 0
    cur.execute("""
        SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS taille
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """)
    row["tables"] = [dict(r) for r in cur.fetchall()]
    conn.close()
    return row


def get_all_games_for_export() -> List[Dict]:
    """Retourne TOUTES les parties triées par date et numéro (pour export Excel)."""
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            date_jeu, numero_jeu, heure,
            gagnant, naturel, est_paire_joueur, est_paire_banquier,
            joueur_total_cartes, joueur_points,
            joueur_carte1_rang, joueur_carte1_ens, joueur_carte1_val,
            joueur_carte2_rang, joueur_carte2_ens, joueur_carte2_val,
            joueur_carte3_rang, joueur_carte3_ens, joueur_carte3_val,
            banquier_total_cartes, banquier_points,
            banquier_carte1_rang, banquier_carte1_ens, banquier_carte1_val,
            banquier_carte2_rang, banquier_carte2_ens, banquier_carte2_val,
            banquier_carte3_rang, banquier_carte3_ens, banquier_carte3_val,
            total_cartes
        FROM parties
        ORDER BY date_jeu ASC, numero_jeu ASC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def init_documentation() -> None:
    """
    Crée la table 'documentation' et y insère la documentation complète
    de la base de données pour qu'un autre bot puisse la lire et l'utiliser.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS documentation (
            id          SERIAL PRIMARY KEY,
            section     VARCHAR(100) NOT NULL,
            cle         VARCHAR(200) NOT NULL,
            valeur      TEXT         NOT NULL,
            mise_a_jour TIMESTAMP    DEFAULT NOW(),
            UNIQUE(section, cle)
        );
    """)

    docs = [
        # ── CONNEXION ────────────────────────────────────────────────────────
        ("connexion", "type",          "PostgreSQL (Render.com)"),
        ("connexion", "driver",        "psycopg2 (Python) | pg (Node.js) | JDBC (Java)"),
        ("connexion", "url_format",    "postgresql://USER:PASSWORD@HOST/DATABASE"),
        ("connexion", "ssl",           "Obligatoire pour Render.com — ajouter ?sslmode=require si necessaire"),
        ("connexion", "port",          "5432 (defaut PostgreSQL)"),
        ("connexion", "timeout",       "connect_timeout=15 recommande"),
        ("connexion", "secret_env",    "Stocker l'URL dans une variable d'environnement RENDER_DB_URL"),

        # ── TABLE PRINCIPALE ─────────────────────────────────────────────────
        ("tables", "parties",
         "Table principale. Chaque ligne = 1 partie de Baccarat 1xBet. "
         "Cle unique : (date_jeu, numero_jeu). Source live toutes les ~60s."),
        ("tables", "documentation",
         "Cette table. Contient la documentation auto-generee lisible par tout bot connecte."),
        ("tables", "vue_historique",
         "Vue SQL lisible avec noms de colonnes clairs. "
         "SELECT * FROM vue_historique WHERE \"Date\" = '2026-03-24' ORDER BY \"Jeu_Numero\""),

        # ── COLONNES TABLE parties ────────────────────────────────────────────
        ("colonnes_parties", "id",                    "SERIAL — cle primaire auto-incrementee"),
        ("colonnes_parties", "date_jeu",               "DATE — date de la partie (fuseau GMT/UTC+0 = Cote d'Ivoire)"),
        ("colonnes_parties", "numero_jeu",             "INTEGER — numero de la partie dans la journee (commence a 1)"),
        ("colonnes_parties", "heure",                  "TIME — heure d'enregistrement en GMT"),
        ("colonnes_parties", "joueur_carte1_rang",     "VARCHAR — rang de la 1re carte joueur (A,2..10,J,Q,K)"),
        ("colonnes_parties", "joueur_carte1_ens",      "VARCHAR — couleur/enseigne : ♠ ♣ ♦ ♥ (Unicode)"),
        ("colonnes_parties", "joueur_carte1_val",      "INTEGER — valeur baccarat (A=1, 2-9=face, 10/J/Q/K=0)"),
        ("colonnes_parties", "joueur_carte2_rang",     "VARCHAR — rang 2e carte joueur"),
        ("colonnes_parties", "joueur_carte2_ens",      "VARCHAR — couleur 2e carte joueur"),
        ("colonnes_parties", "joueur_carte2_val",      "INTEGER — valeur baccarat 2e carte joueur"),
        ("colonnes_parties", "joueur_carte3_rang",     "VARCHAR — rang 3e carte joueur (NULL si 2 cartes seulement)"),
        ("colonnes_parties", "joueur_carte3_ens",      "VARCHAR — couleur 3e carte joueur (NULL si absente)"),
        ("colonnes_parties", "joueur_carte3_val",      "INTEGER — valeur 3e carte joueur (NULL si absente)"),
        ("colonnes_parties", "joueur_total_cartes",    "INTEGER — nombre total de cartes du joueur (2 ou 3)"),
        ("colonnes_parties", "joueur_points",          "INTEGER — total des points baccarat du joueur (0 a 9)"),
        ("colonnes_parties", "banquier_carte1_rang",   "VARCHAR — rang 1re carte banquier"),
        ("colonnes_parties", "banquier_carte1_ens",    "VARCHAR — couleur 1re carte banquier"),
        ("colonnes_parties", "banquier_carte1_val",    "INTEGER — valeur baccarat 1re carte banquier"),
        ("colonnes_parties", "banquier_carte2_rang",   "VARCHAR — rang 2e carte banquier"),
        ("colonnes_parties", "banquier_carte2_ens",    "VARCHAR — couleur 2e carte banquier"),
        ("colonnes_parties", "banquier_carte2_val",    "INTEGER — valeur baccarat 2e carte banquier"),
        ("colonnes_parties", "banquier_carte3_rang",   "VARCHAR — rang 3e carte banquier (NULL si 2 cartes)"),
        ("colonnes_parties", "banquier_carte3_ens",    "VARCHAR — couleur 3e carte banquier (NULL si absente)"),
        ("colonnes_parties", "banquier_carte3_val",    "INTEGER — valeur 3e carte banquier (NULL si absente)"),
        ("colonnes_parties", "banquier_total_cartes",  "INTEGER — nombre total de cartes du banquier (2 ou 3)"),
        ("colonnes_parties", "banquier_points",        "INTEGER — total des points baccarat du banquier (0 a 9)"),
        ("colonnes_parties", "gagnant",                "VARCHAR — resultat : 'Joueur' | 'Banquier' | 'Egalite'"),
        ("colonnes_parties", "total_cartes",           "INTEGER — total cartes de la partie (4, 5 ou 6)"),
        ("colonnes_parties", "est_paire_joueur",       "BOOLEAN — vrai si carte1 et carte2 joueur ont meme rang"),
        ("colonnes_parties", "est_paire_banquier",     "BOOLEAN — vrai si carte1 et carte2 banquier ont meme rang"),
        ("colonnes_parties", "naturel",                "BOOLEAN — vrai si l'un des cotes totalise 8 ou 9 en 2 cartes"),
        ("colonnes_parties", "joueur_troisieme",       "BOOLEAN — vrai si le joueur a recu une 3e carte"),
        ("colonnes_parties", "banquier_troisieme",     "BOOLEAN — vrai si le banquier a recu une 3e carte"),
        ("colonnes_parties", "enregistre_le",          "TIMESTAMP — horodatage d'insertion en base"),

        # ── VALEURS DE REFERENCE ─────────────────────────────────────────────
        ("valeurs", "gagnant_joueur",    "Joueur — le joueur a gagne la partie"),
        ("valeurs", "gagnant_banquier",  "Banquier — le banquier a gagne la partie"),
        ("valeurs", "gagnant_egalite",   "Egalite — les deux cotes ont le meme total (nulle)"),
        ("valeurs", "enseignes",         "♠=Pique  ♣=Trefle  ♦=Carreau  ♥=Coeur"),
        ("valeurs", "rangs",             "A(=1) 2 3 4 5 6 7 8 9 10 J Q K"),
        ("valeurs", "valeurs_baccarat",  "A=1, 2=2, ..., 9=9, 10=0, J=0, Q=0, K=0"),
        ("valeurs", "total_points",      "Somme des valeurs baccarat modulo 10 (ex: 8+5=13 => 3 pts)"),
        ("valeurs", "seuil_plus65",      "total >= 7 = Plus de 6.5 (parie haute)"),
        ("valeurs", "seuil_moins45",     "total <= 4 = Moins de 4.5 (parie basse)"),
        ("valeurs", "nb_cartes_2v2",     "2 cartes joueur + 2 cartes banquier = 4 cartes total"),
        ("valeurs", "nb_cartes_3v2",     "3 cartes joueur + 2 cartes banquier = 5 cartes total"),
        ("valeurs", "nb_cartes_2v3",     "2 cartes joueur + 3 cartes banquier = 5 cartes total"),
        ("valeurs", "nb_cartes_3v3",     "3 cartes joueur + 3 cartes banquier = 6 cartes total"),

        # ── REQUETES SQL UTILES ───────────────────────────────────────────────
        ("requetes_sql", "lire_doc",
         "SELECT section, cle, valeur FROM documentation ORDER BY section, cle;"),

        ("requetes_sql", "parties_du_jour",
         "SELECT * FROM parties WHERE date_jeu = CURRENT_DATE ORDER BY numero_jeu;"),

        ("requetes_sql", "parties_par_date",
         "SELECT * FROM parties WHERE date_jeu = '2026-03-24' ORDER BY numero_jeu;"),

        ("requetes_sql", "derniere_partie",
         "SELECT * FROM parties ORDER BY date_jeu DESC, numero_jeu DESC LIMIT 1;"),

        ("requetes_sql", "stats_du_jour",
         "SELECT COUNT(*) AS total, "
         "COUNT(*) FILTER (WHERE gagnant='Joueur') AS joueur, "
         "COUNT(*) FILTER (WHERE gagnant='Banquier') AS banquier, "
         "COUNT(*) FILTER (WHERE gagnant='Egalite') AS egalite "
         "FROM parties WHERE date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "total_global",
         "SELECT COUNT(*) AS total, COUNT(DISTINCT date_jeu) AS jours, "
         "MIN(date_jeu) AS debut, MAX(date_jeu) AS fin FROM parties;"),

        ("requetes_sql", "parties_joueur_gagne",
         "SELECT * FROM parties WHERE gagnant = 'Joueur' AND date_jeu = '2026-03-24' ORDER BY numero_jeu;"),

        ("requetes_sql", "parties_banquier_gagne",
         "SELECT * FROM parties WHERE gagnant = 'Banquier' AND date_jeu = '2026-03-24' ORDER BY numero_jeu;"),

        ("requetes_sql", "parties_egalite",
         "SELECT * FROM parties WHERE gagnant = 'Egalite' ORDER BY date_jeu DESC, numero_jeu DESC;"),

        ("requetes_sql", "naturels_du_jour",
         "SELECT * FROM parties WHERE naturel = true AND date_jeu = CURRENT_DATE ORDER BY numero_jeu;"),

        ("requetes_sql", "plus_65_joueur",
         "SELECT * FROM parties WHERE joueur_points >= 7 AND date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "moins_45_joueur",
         "SELECT * FROM parties WHERE joueur_points <= 4 AND date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "plus_65_banquier",
         "SELECT * FROM parties WHERE banquier_points >= 7 AND date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "moins_45_banquier",
         "SELECT * FROM parties WHERE banquier_points <= 4 AND date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "distribution_points_joueur",
         "SELECT joueur_points, COUNT(*) FROM parties "
         "WHERE date_jeu = CURRENT_DATE GROUP BY joueur_points ORDER BY joueur_points;"),

        ("requetes_sql", "distribution_points_banquier",
         "SELECT banquier_points, COUNT(*) FROM parties "
         "WHERE date_jeu = CURRENT_DATE GROUP BY banquier_points ORDER BY banquier_points;"),

        ("requetes_sql", "3e_carte_joueur",
         "SELECT * FROM parties WHERE joueur_troisieme = true AND date_jeu = CURRENT_DATE;"),

        ("requetes_sql", "taille_base",
         "SELECT pg_size_pretty(pg_database_size(current_database())) AS taille_db, "
         "pg_size_pretty(pg_total_relation_size('parties')) AS taille_parties, "
         "COUNT(*) AS total_lignes FROM parties;"),

        ("requetes_sql", "dates_disponibles",
         "SELECT DISTINCT date_jeu, COUNT(*) AS nb_parties "
         "FROM parties GROUP BY date_jeu ORDER BY date_jeu DESC;"),

        # ── LOGIQUE METIER ────────────────────────────────────────────────────
        ("logique_metier", "capture_frequence",
         "Le bot capture automatiquement les parties 1xBet Live Baccarat toutes les ~60 secondes."),
        ("logique_metier", "fuseau_horaire",
         "GMT (UTC+0) = fuseau Cote d'Ivoire. Toutes les heures en base sont en GMT."),
        ("logique_metier", "numero_jeu_reset",
         "Le numero_jeu repart de 1 chaque nouvelle journee (date_jeu)."),
        ("logique_metier", "cle_unique",
         "La paire (date_jeu, numero_jeu) est unique. Insertion ignoree si doublon."),
        ("logique_metier", "source",
         "Donnees issues du flux live 1xBet Baccarat via Telegram (scraping de messages)."),
        ("logique_metier", "bot_principal",
         "Le bot principal (ID 7815360317) capture et stocke. "
         "Un bot secondaire peut se connecter en lecture seule a la meme base."),
    ]

    for section, cle, valeur in docs:
        cur.execute("""
            INSERT INTO documentation (section, cle, valeur)
            VALUES (%s, %s, %s)
            ON CONFLICT (section, cle) DO UPDATE SET valeur = EXCLUDED.valeur, mise_a_jour = NOW();
        """, (section, cle, valeur))

    conn.commit()
    conn.close()
    logger.info("Documentation insérée/mise à jour dans la base")


def get_documentation(section: Optional[str] = None) -> List[Dict]:
    """Lit la documentation stockée dans la base."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if section:
        cur.execute("SELECT section, cle, valeur FROM documentation WHERE section = %s ORDER BY cle", (section,))
    else:
        cur.execute("SELECT section, cle, valeur FROM documentation ORDER BY section, cle")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_public_view():
    """Crée une vue lisible pour consultation externe de la base."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE OR REPLACE VIEW vue_historique AS
        SELECT
            date_jeu                                          AS "Date",
            numero_jeu                                        AS "Jeu_Numero",
            heure                                             AS "Heure_GMT",
            -- Joueur
            CONCAT(joueur_carte1_rang, joueur_carte1_ens)     AS "J_Carte1",
            joueur_carte1_val                                 AS "J_C1_Val",
            CONCAT(joueur_carte2_rang, joueur_carte2_ens)     AS "J_Carte2",
            joueur_carte2_val                                 AS "J_C2_Val",
            COALESCE(CONCAT(joueur_carte3_rang, joueur_carte3_ens), '-') AS "J_Carte3",
            joueur_carte3_val                                 AS "J_C3_Val",
            joueur_total_cartes                               AS "J_Nb_Cartes",
            joueur_points                                     AS "J_Points",
            -- Banquier
            CONCAT(banquier_carte1_rang, banquier_carte1_ens) AS "B_Carte1",
            banquier_carte1_val                               AS "B_C1_Val",
            CONCAT(banquier_carte2_rang, banquier_carte2_ens) AS "B_Carte2",
            banquier_carte2_val                               AS "B_C2_Val",
            COALESCE(CONCAT(banquier_carte3_rang, banquier_carte3_ens), '-') AS "B_Carte3",
            banquier_carte3_val                               AS "B_C3_Val",
            banquier_total_cartes                             AS "B_Nb_Cartes",
            banquier_points                                   AS "B_Points",
            -- Résultat
            gagnant                                           AS "Gagnant",
            total_cartes                                      AS "Total_Cartes",
            CASE WHEN est_paire_joueur   THEN 'Oui' ELSE 'Non' END AS "Paire_Joueur",
            CASE WHEN est_paire_banquier THEN 'Oui' ELSE 'Non' END AS "Paire_Banquier",
            CASE WHEN naturel            THEN 'Oui' ELSE 'Non' END AS "Naturel",
            CASE WHEN joueur_troisieme   THEN 'Oui' ELSE 'Non' END AS "3e_Carte_Joueur",
            CASE WHEN banquier_troisieme THEN 'Oui' ELSE 'Non' END AS "3e_Carte_Banquier",
            enregistre_le                                     AS "Enregistre_Le"
        FROM parties
        ORDER BY date_jeu DESC, numero_jeu ASC;
    """)
    conn.commit()
    conn.close()
    logger.info("Vue vue_historique créée")
