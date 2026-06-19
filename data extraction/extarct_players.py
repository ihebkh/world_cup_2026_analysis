#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import sys
import time
import requests
from datetime import datetime

API_TOKEN = os.environ.get("FOOTBALL_DATA_API_TOKEN", "0a5763bab4a2469d8b5bf1b089edaaea")
BASE_URL = "https://api.football-data.org/v4"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
TEAMS_CSV = os.path.join(DATA_DIR, "equipes.csv")
PLAYERS_CSV = os.path.join(DATA_DIR, "joueurs.csv")
DELAY_BETWEEN_CALLS = 6.5

PLAYERS_FIELDNAMES = [
    "joueur_id", "team_id", "equipe", "nom", "prenom", "nom_complet",
    "date_naissance", "nationalite", "position", "numero_maillot",
    "derniere_maj"
]


def get_session():
    session = requests.Session()
    session.headers.update({"X-Auth-Token": API_TOKEN})
    return session


def call_api(session, endpoint, params=None, retry_count=0):
    url = f"{BASE_URL}{endpoint}"
    try:
        response = session.get(url, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"[ERREUR RESEAU] {endpoint} -> {e}")
        return None

    if response.status_code == 200:
        return response.json()

    if response.status_code in (401, 403):
        print(f"[ERREUR AUTH/PLAN] Code {response.status_code} sur {endpoint}")
        print("Token invalide OU squad non inclus dans votre plan football-data.org.")
        print(response.text[:300])
        return None

    if response.status_code == 404:
        print(f"[INFO] Ressource introuvable : {endpoint}")
        return None

    if response.status_code == 429:
        if retry_count >= 2:
            print("[ERREUR] Limite API atteinte apres plusieurs tentatives.")
            return None
        print("[LIMITE API] Pause 60 secondes...")
        time.sleep(60)
        return call_api(session, endpoint, params, retry_count + 1)

    print(f"[ERREUR {response.status_code}] {endpoint} -> {response.text[:300]}")
    return None


def safe_get(dictionary, *keys, default=""):
    current = dictionary
    for key in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
    return current if current is not None else default


def write_csv(filepath, fieldnames, rows, mode="w"):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    write_header = mode == "w" or not os.path.exists(filepath)
    with open(filepath, mode, newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_teams():
    if not os.path.exists(TEAMS_CSV):
        print(f"[ERREUR] Fichier introuvable : {TEAMS_CSV}")
        print("Lancez d'abord extract_wc2026.py pour generer equipes.csv.")
        sys.exit(1)

    teams = []
    with open(TEAMS_CSV, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("team_id"):
                teams.append(row)

    if not teams:
        print("[ERREUR] Aucune equipe trouvee dans equipes.csv.")
        sys.exit(1)

    return teams


def extract_players_for_team(session, team_id, team_name):
    data = call_api(session, f"/teams/{team_id}")

    if not data or "squad" not in data:
        print(f"  -> Aucun joueur trouve pour {team_name} (id {team_id}).")
        return []

    rows = []
    for player in data["squad"]:
        prenom = safe_get(player, "firstName")
        nom = safe_get(player, "lastName")
        nom_complet = safe_get(player, "name")
        rows.append({
            "joueur_id": safe_get(player, "id"),
            "team_id": team_id,
            "equipe": team_name,
            "nom": nom,
            "prenom": prenom,
            "nom_complet": nom_complet,
            "date_naissance": safe_get(player, "dateOfBirth"),
            "nationalite": safe_get(player, "nationality"),
            "position": safe_get(player, "position"),
            "numero_maillot": safe_get(player, "shirtNumber"),
            "derniere_maj": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    print(f"  -> {len(rows)} joueurs trouves pour {team_name}.")
    return rows


def main():
    if not API_TOKEN or API_TOKEN == "VOTRE_CLE_API_ICI":
        print("[ERREUR] Renseignez votre cle API dans API_TOKEN ou FOOTBALL_DATA_API_TOKEN.")
        sys.exit(1)

    teams = read_teams()
    print(f"{len(teams)} equipes chargees depuis equipes.csv.")

    session = get_session()

    # Ecrit l'entete une seule fois, puis ajoute les joueurs equipe par equipe
    write_csv(PLAYERS_CSV, PLAYERS_FIELDNAMES, [], mode="w")

    total_joueurs = 0
    for index, team in enumerate(teams, start=1):
        team_id = team["team_id"]
        team_name = team.get("nom") or team.get("nom_court") or team_id
        print(f"[{index}/{len(teams)}] Extraction des joueurs : {team_name}...")

        rows = extract_players_for_team(session, team_id, team_name)
        if rows:
            write_csv(PLAYERS_CSV, PLAYERS_FIELDNAMES, rows, mode="a")
            total_joueurs += len(rows)

        if index < len(teams):
            time.sleep(DELAY_BETWEEN_CALLS)

    print(f"[OK] {total_joueurs} joueurs ecrits -> {PLAYERS_CSV}")
    print("Extraction terminee.")


if __name__ == "__main__":
    main()