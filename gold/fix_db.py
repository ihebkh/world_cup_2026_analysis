"""
fix_db.py — Recrée le schéma du data warehouse worldcup2026.

Usage :
    pip install psycopg2-binary
    cd gold
    python fix_db.py
"""

import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 non installé. Lancez : pip install psycopg2-binary")
    sys.exit(1)

# ── config ────────────────────────────────────────────────────
PG_HOST     = "localhost"
PG_PORT     = "5432"
PG_DATABASE = "worldcup2026"
PG_USER     = "postgres"
PG_PASSWORD = "admin"

# ── DDL complet avec les bonnes tailles de colonnes ───────────
DDL = """
-- Suppression dans l'ordre inverse des dépendances FK
DROP TABLE IF EXISTS public.fact_classement  CASCADE;
DROP TABLE IF EXISTS public.fact_buteur       CASCADE;
DROP TABLE IF EXISTS public.fact_match        CASCADE;
DROP TABLE IF EXISTS public.dim_date          CASCADE;
DROP TABLE IF EXISTS public.dim_stade         CASCADE;
DROP TABLE IF EXISTS public.dim_joueur        CASCADE;
DROP TABLE IF EXISTS public.dim_equipe        CASCADE;

-- dim_equipe
CREATE TABLE public.dim_equipe (
    equipe_id                   BIGINT          NOT NULL,
    nom_equipe                  VARCHAR(100),
    nom_court                   VARCHAR(50),
    code_tla                    VARCHAR(3),
    pays                        VARCHAR(100),
    code_pays                   VARCHAR(10),
    logo_url                    TEXT,
    couleurs_club               VARCHAR(100),
    site_web                    VARCHAR(200),
    selectionneur               VARCHAR(100),
    nationalite_selectionneur   VARCHAR(100),
    derniere_maj                TIMESTAMP,
    CONSTRAINT pk_dim_equipe PRIMARY KEY (equipe_id)
);

-- dim_joueur
CREATE TABLE public.dim_joueur (
    joueur_id       BIGINT      NOT NULL,
    equipe_id       BIGINT,
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    nom_complet     VARCHAR(200),
    date_naissance  DATE,
    nationalite     VARCHAR(100),
    position        VARCHAR(50),
    numero_maillot  INTEGER,
    derniere_maj    TIMESTAMP,
    CONSTRAINT pk_dim_joueur    PRIMARY KEY (joueur_id),
    CONSTRAINT fk_joueur_equipe FOREIGN KEY (equipe_id)
        REFERENCES public.dim_equipe (equipe_id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- dim_stade
CREATE TABLE public.dim_stade (
    stade_id    INTEGER     NOT NULL,
    nom_stade   VARCHAR(200),
    ville       VARCHAR(100),
    CONSTRAINT pk_dim_stade PRIMARY KEY (stade_id)
);

-- dim_date
CREATE TABLE public.dim_date (
    date_id             INTEGER NOT NULL,
    date_complete       DATE,
    jour                INTEGER,
    mois                INTEGER,
    nom_mois            VARCHAR(20),
    annee               INTEGER,
    num_jour_semaine    INTEGER,
    nom_jour_semaine    VARCHAR(20),
    semaine_annee       INTEGER,
    CONSTRAINT pk_dim_date   PRIMARY KEY (date_id),
    CONSTRAINT chk_date_mois CHECK (mois BETWEEN 1 AND 12),
    CONSTRAINT chk_date_jour CHECK (jour BETWEEN 1 AND 31),
    CONSTRAINT chk_date_dow  CHECK (num_jour_semaine BETWEEN 1 AND 7)
);

-- fact_match
CREATE TABLE public.fact_match (
    match_id                 BIGINT  NOT NULL,
    date_id                  INTEGER,
    equipe_domicile_id       BIGINT,
    equipe_exterieur_id      BIGINT,
    vainqueur_equipe_id      BIGINT,
    stade_id                 INTEGER,
    competition              VARCHAR(100),
    journee                  INTEGER,
    statut                   VARCHAR(50),
    date_heure               TIMESTAMP,
    score_domicile_mi_temps  INTEGER,
    score_exterieur_mi_temps INTEGER,
    score_domicile_final     INTEGER,
    score_exterieur_final    INTEGER,
    total_buts               INTEGER,
    resultat_match           VARCHAR(10),
    is_match_joue            BOOLEAN,
    arbitre                  VARCHAR(100),
    derniere_maj             TIMESTAMP,
    CONSTRAINT pk_fact_match      PRIMARY KEY (match_id),
    CONSTRAINT fk_match_date      FOREIGN KEY (date_id)
        REFERENCES public.dim_date (date_id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_match_dom       FOREIGN KEY (equipe_domicile_id)
        REFERENCES public.dim_equipe (equipe_id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_match_ext       FOREIGN KEY (equipe_exterieur_id)
        REFERENCES public.dim_equipe (equipe_id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_match_vainqueur FOREIGN KEY (vainqueur_equipe_id)
        REFERENCES public.dim_equipe (equipe_id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_match_stade     FOREIGN KEY (stade_id)
        REFERENCES public.dim_stade (stade_id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT chk_resultat       CHECK (resultat_match IN ('HOME_WIN','AWAY_WIN','DRAW') OR resultat_match IS NULL)
);

-- fact_buteur
CREATE TABLE public.fact_buteur (
    joueur_id       BIGINT       NOT NULL,
    equipe_id       BIGINT       NOT NULL,
    buts            INTEGER      DEFAULT 0,
    assists         INTEGER      DEFAULT 0,
    matchs_joues    INTEGER      DEFAULT 0,
    penaltys        INTEGER      DEFAULT 0,
    buts_par_match  NUMERIC(5,2) DEFAULT 0.00,
    derniere_maj    TIMESTAMP,
    CONSTRAINT pk_fact_buteur       PRIMARY KEY (joueur_id, equipe_id),
    CONSTRAINT fk_buteur_joueur     FOREIGN KEY (joueur_id)
        REFERENCES public.dim_joueur (joueur_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_buteur_equipe     FOREIGN KEY (equipe_id)
        REFERENCES public.dim_equipe (equipe_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_buts_positif     CHECK (buts >= 0),
    CONSTRAINT chk_assists_positif  CHECK (assists >= 0),
    CONSTRAINT chk_penaltys_positif CHECK (penaltys >= 0)
);

-- fact_classement  (groupe VARCHAR(50) pour valeurs comme 'GROUP_STAGE')
CREATE TABLE public.fact_classement (
    equipe_id           BIGINT      NOT NULL,
    groupe              VARCHAR(50) NOT NULL,
    position            INTEGER,
    matchs_joues        INTEGER     DEFAULT 0,
    victoires           INTEGER     DEFAULT 0,
    nuls                INTEGER     DEFAULT 0,
    defaites            INTEGER     DEFAULT 0,
    buts_pour           INTEGER     DEFAULT 0,
    buts_contre         INTEGER     DEFAULT 0,
    difference_buts     INTEGER     DEFAULT 0,
    points              INTEGER     DEFAULT 0,
    forme               VARCHAR(20),
    derniere_maj        TIMESTAMP,
    CONSTRAINT pk_fact_classement   PRIMARY KEY (equipe_id, groupe),
    CONSTRAINT fk_classement_equipe FOREIGN KEY (equipe_id)
        REFERENCES public.dim_equipe (equipe_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_points_positif   CHECK (points >= 0),
    CONSTRAINT chk_matchs_positif   CHECK (matchs_joues >= 0)
);

-- Index
CREATE INDEX idx_fact_match_date        ON public.fact_match (date_id);
CREATE INDEX idx_fact_match_dom         ON public.fact_match (equipe_domicile_id);
CREATE INDEX idx_fact_match_ext         ON public.fact_match (equipe_exterieur_id);
CREATE INDEX idx_fact_match_stade       ON public.fact_match (stade_id);
CREATE INDEX idx_fact_match_statut      ON public.fact_match (statut);
CREATE INDEX idx_fact_match_joue        ON public.fact_match (is_match_joue);
CREATE INDEX idx_fact_buteur_equipe     ON public.fact_buteur (equipe_id);
CREATE INDEX idx_fact_buteur_buts       ON public.fact_buteur (buts DESC);
CREATE INDEX idx_fact_classement_groupe ON public.fact_classement (groupe);
CREATE INDEX idx_fact_classement_points ON public.fact_classement (points DESC);
CREATE INDEX idx_dim_joueur_equipe      ON public.dim_joueur (equipe_id);
CREATE INDEX idx_dim_joueur_position    ON public.dim_joueur (position);
CREATE INDEX idx_dim_date_annee         ON public.dim_date (annee);
CREATE INDEX idx_dim_date_mois          ON public.dim_date (annee, mois);
"""

# ── connexion et exécution ────────────────────────────────────

def main():
    print(f"Connexion à {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE} ...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        conn.autocommit = True
    except psycopg2.OperationalError as e:
        print(f"Erreur de connexion : {e}")
        print("Vérifiez que PostgreSQL tourne et que les credentials dans fix_db.py sont corrects.")
        sys.exit(1)

    print("Connexion OK. Application du schéma...")
    cur = conn.cursor()

    statements = [s.strip() for s in DDL.split(";") if s.strip()]
    errors = []

    for stmt in statements:
        first_line = stmt.splitlines()[0].strip()
        try:
            cur.execute(stmt)
            print(f"  OK  — {first_line[:80]}")
        except Exception as e:
            print(f"  ERR — {first_line[:80]}")
            print(f"        {e}")
            errors.append((first_line, str(e)))

    cur.close()
    conn.close()

    print()
    if errors:
        print(f"Terminé avec {len(errors)} erreur(s) :")
        for stmt, err in errors:
            print(f"  - {stmt[:60]} → {err}")
        sys.exit(1)
    else:
        print("Schéma appliqué avec succès — toutes les tables sont prêtes.")


if __name__ == "__main__":
    main()
