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
COMPETITION_CODE = "WC"
SEASON = 2026
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DELAY_BETWEEN_CALLS = 6.5

FIELDNAMES = [
    "joueur_id", "nom", "nationalite", "position", "date_naissance",
    "equipe_id", "equipe", "buts", "assists", "matchs_joues", "penaltys",
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
        print(f"[ERREUR AUTH/PLAN] Code {response.status_code}")
        print("Verifiez votre cle API ou votre plan football-data.org.")
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


def write_csv(filepath, fieldnames, rows):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[OK] {len(rows)} lignes ecrites -> {filepath}")


def extract_scorers(session):
    print("Extraction des buteurs...")
    data = call_api(
        session,
        f"/competitions/{COMPETITION_CODE}/scorers",
        params={"season": SEASON, "limit": 100}
    )
    time.sleep(DELAY_BETWEEN_CALLS)

    if not data or "scorers" not in data:
        print("Aucun buteur trouve.")
        write_csv(os.path.join(OUTPUT_DIR, "buteurs.csv"), FIELDNAMES, [])
        return []

    rows = []
    for entry in data["scorers"]:
        player = entry.get("player", {})
        team = entry.get("team", {})
        rows.append({
            "joueur_id": safe_get(player, "id"),
            "nom": safe_get(player, "name"),
            "nationalite": safe_get(player, "nationality"),
            "position": safe_get(player, "position"),
            "date_naissance": safe_get(player, "dateOfBirth"),
            "equipe_id": safe_get(team, "id"),
            "equipe": safe_get(team, "name"),
            "buts": safe_get(entry, "goals"),
            "assists": safe_get(entry, "assists"),
            "matchs_joues": safe_get(entry, "playedMatches"),
            "penaltys": safe_get(entry, "penalties"),
            "derniere_maj": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    write_csv(os.path.join(OUTPUT_DIR, "buteurs.csv"), FIELDNAMES, rows)
    return rows


def main():
    if not API_TOKEN:
        print("[ERREUR] Renseignez votre cle API dans la variable FOOTBALL_DATA_API_TOKEN.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = get_session()
    extract_scorers(session)
    print("Extraction terminee.")


if __name__ == "__main__":
    main()