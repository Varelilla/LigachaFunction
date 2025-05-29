import os
from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, firestore
import requests
import re
import time
import datetime
import pandas as pd
from google.api_core.retry import Retry
import json

app = Flask(__name__)

# Inicializar Firebase
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# 🔥 Ligas a Monitorear
LEAGUES = {
    "PD": "esp.1",               # La Liga (España)
    "PL": "eng.1",               # Premier League (Inglaterra)
    "BL1": "ger.1",              # Bundesliga (Alemania)
    "FL1": "fra.1",              # Ligue 1 (Francia)
    "SA": "ita.1",               # Serie A (Italia)
    "CL": "uefa.champions",      # Champions League
    "EL": "uefa.europa",         # Europa League
    "WC": "fifa.world",          # Mundial
    "EC": "uefa.euro",           # Eurocopa
    "CA": "conmebol.copa",       # Copa América
    "CAF": "caf.caf"             # Copa África
}


API_KEY = "789235b2ab894e28b3f32e731a416675"  # Reemplázalo con tu API Key real
API_URL = "https://api.football-data.org/v4/"

@app.route("/update_teams", methods=["GET"])
def update_teams(request):
    try:
        LEAGUES = [
            "esp.1",           # LaLiga
            "eng.1",           # Premier League
            "ger.1",           # Bundesliga
            "fra.1",           # Ligue 1
            "ita.1",           # Serie A
            "uefa.champions",  # Champions League
            "uefa.europa",     # Europa League
            "fifa.world",      # Mundial
            "uefa.euro",       # Eurocopa
            "conmebol.copa",   # Copa América
            "caf.nations"      # Copa África
        ]

        for league in LEAGUES:
            url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/teams"
            response = requests.get(url)

            if response.status_code != 200:
                print(f"⚠️ Falló la liga {league} con status {response.status_code}")
                continue

            data = response.json()
            teams = data["sports"][0]["leagues"][0].get("teams", [])

            for team_data in teams:
                team = team_data["team"]
                team_id = team["id"]

                # Referencia al documento del equipo
                doc_ref = db.collection("sports").document("football").collection("teams").document(str(team_id))
                doc_snapshot = doc_ref.get()

                if doc_snapshot.exists:
                    existing_data = doc_snapshot.to_dict()
                    leagues = existing_data.get("leagues", [])
                    if league not in leagues:
                        leagues.append(league)
                        print(f"➕ Añadiendo {league} a {team['name']}")
                else:
                    leagues = [league]

                team_doc = {
                    "id": team_id,
                    "name": team.get("name"),
                    "displayName": team.get("displayName"),
                    "shortName": team.get("shortDisplayName"),
                    "abbreviation": team.get("abbreviation"),
                    "slug": team.get("slug"),
                    "location": team.get("location"),
                    "color": team.get("color"),
                    "alternateColor": team.get("alternateColor"),
                    "logo": next((logo["href"] for logo in team.get("logos", []) if "dark" not in logo["href"]), None),
                    "logoDark": next((logo["href"] for logo in team.get("logos", []) if "dark" in logo["href"]), None),
                    "isActive": team.get("isActive", True),
                    "sport": "football",
                    "leagues": leagues
                }

                doc_ref.set(team_doc, merge=True)
                print(f"✅ Team updated: {team_doc['name']} ({league})")

        return "✅ All teams updated from ESPN API", 200

    except Exception as e:
        print(f"❌ Error updating teams: {e}")
        return str(e), 500


@app.route("/update_players", methods=["GET"])
def update_players(request):
    try:
        teams_ref = db.collection("sports").document("football").collection("teams")
        page_size = 100
        last_doc = None

        while True:
            query = teams_ref.limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)

            teams = list(query.stream())

            if not teams:
                break  # No hay más equipos

            for team_doc in teams:
                team_data = team_doc.to_dict()
                team_id = team_data["id"]
                league = team_data.get("league", "unknown")

                print(f"🔍 Getting players from {team_data['name']} ({league})")

                url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/teams/{team_id}/roster"
                response = requests.get(url)

                if response.status_code != 200:
                    print(f"❌ Error fetching roster for team {team_id}")
                    continue

                data = response.json()
                players = data.get("athletes", [])

                print(f"👥 {len(players)} players found for {team_data['name']}")

                for player in players:
                    player_id = player["id"]

                    # ✨ Estadísticas actuales
                    stats_raw = player.get("statistics", {}).get("splits", {}).get("categories", [])
                    stats_clean = []
                    for category in stats_raw:
                        for stat in category.get("stats", []):
                            stats_clean.append({
                                "name": stat.get("name"),
                                "displayName": stat.get("displayName"),
                                "value": stat.get("value")
                            })

                    # 💢 Lesiones activas (si hay)
                    injuries = [injury.get("description") for injury in player.get("injuries", []) if "description" in injury]

                    player_doc = {
                        "id": player_id,
                        "fullName": player.get("fullName"),
                        "displayName": player.get("displayName"),
                        "shortName": player.get("shortName"),
                        "jersey": player.get("jersey"),
                        "position": player.get("position", {}).get("abbreviation"),
                        "teamId": team_id,
                        "teamName": team_data.get("name"),
                        "league": league,
                        "sport": "football",
                        "age": player.get("age"),
                        "height": player.get("displayHeight"),
                        "weight": player.get("displayWeight"),
                        "headshot": player.get("headshot", {}).get("href"),
                        "statistics": stats_clean,
                        "injuries": injuries
                    }

                    doc_ref = db.collection("sports").document("football").collection("players").document(str(player_id))
                    doc_ref.set(player_doc, merge=True)

                    print(f"✅ Player added: {player_doc['displayName']} ({player_id})")

            # Seteamos el último doc procesado para la siguiente página
            last_doc = teams[-1]

        return "✅ All players (paginated) updated", 200

    except Exception as e:
        print(f"❌ Error updating players: {e}")
        return str(e), 500

    
@app.route("/update_leagues", methods=["GET"])
def update_leagues(request):
    LEAGUES = [
        "esp.1",
        "eng.1",
        "ger.1",
        "ita.1",
        "fra.1",
        "uefa.champions",
        "uefa.europa",
        "fifa.world",
        "uefa.euro",
        "caf.caf",
        "conmebol.copa"
    ]

    for league_code in LEAGUES:
        url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_code}/scoreboard"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"❌ No data for league {league_code} (status {response.status_code})")
            continue

        data = response.json()
        leagues_info = data.get("leagues", [])
        league_info = data.get("leagues", [])[0]
        season = league_info.get("season", {})

        if not leagues_info:
            print(f"⚠️ No league data found for {league_code}")
            continue

        league_info = leagues_info[0]  # Siempre viene una sola liga por scoreboard

        logos = league_info.get("logos", [])
        logo = next((l.get("href") for l in logos if "dark" not in l.get("href", "")), None)
        dark_logo = next((l.get("href") for l in logos if "dark" in l.get("href", "")), None)
        # printeamos las fechas a ver si estan bien porque por alguna razon "startDate": "2024-06-01T04:00Z" pero me sale none
        print(f"League: {league_code}, Start Date: {season.get('startDate')}, End Date: {season.get('endDate')}")
        # printeamos la season
        print(f"League: {league_code}, Season: {season}")

        
        league_doc = {
            "id": f"{league_code}__{season.get('year')}",
            "code": league_code,
            "seasonYear": season.get("year"),
            "seasonLabel": season.get("displayName"),
            "startDate": str(season.get("startDate")) if season.get("startDate") else None,
            "endDate": str(season.get("endDate")) if season.get("endDate") else None,
            "name": league_info.get("name"),
            "abbreviation": league_info.get("abbreviation"),
            "logo": logo,
            "darkLogo": dark_logo,
            "active": True
        }

        db.collection("sports").document("football").collection("leagues").document(league_doc["id"]).set(league_doc, merge=True)
        print(f"✅ League added: {league_doc['id']}")

    return "✅ Leagues updated", 200


@app.route("/update_matchweeks_fallback", methods=["GET"])
def update_matchweeks_fallback(request):
    try:
        for league_code, league_id in LEAGUES.items():
            print(f"⚽ Checking {league_code} ({league_id})...")

            url_competition = f"{API_URL}competitions/{league_code}"
            HEADERS = {"X-Auth-Token": API_KEY}
            response = requests.get(url_competition, headers=HEADERS)
            time.sleep(5)

            if response.status_code != 200:
                print(f"⚠️ Error getting competition info for {league_id}")
                continue

            competition_data = response.json()
            current_matchday = competition_data["currentSeason"].get("currentMatchday")
            season_start = competition_data["currentSeason"]["startDate"]
            season_year = season_start[:4]

            if not current_matchday:
                print(f"⚠️ No current matchday found for {league_code}")
                continue

            matchdays = [current_matchday]
            if current_matchday > 1:
                matchdays.append(current_matchday - 1)
            matchdays.append(current_matchday + 1)

            for matchday in matchdays:
                url_matches = f"{API_URL}competitions/{league_code}/matches?matchday={matchday}"
                response_matches = requests.get(url_matches, headers=HEADERS)
                time.sleep(5)

                if response_matches.status_code != 200:
                    print(f"⚠️ Error fetching matchday {matchday}")
                    continue

                data = response_matches.json()
                matches = data.get("matches", [])
                if not matches:
                    print(f"⚠️ No matches for matchday {matchday}")
                    continue

                matchweek_id = f"{league_id}_{season_year}_{matchday}"
                matches_list = []

                for match in matches:
                    home_name = match["homeTeam"]["name"]
                    away_name = match["awayTeam"]["name"]
                    match_date = match["utcDate"]
                    venue = match.get("venue", None)

                    # Buscar IDs ESPN desde team_links
                    home_doc = db.collection("sports").document("football").collection("team_links").document(
                        home_name.lower().replace(" ", "").replace(".", "").replace("-", "").replace("'", "")
                    ).get()
                    away_doc = db.collection("sports").document("football").collection("team_links").document(
                        away_name.lower().replace(" ", "").replace(".", "").replace("-", "").replace("'", "")
                    ).get()

                    home_id = home_doc.to_dict().get("espn_id") if home_doc.exists else None
                    away_id = away_doc.to_dict().get("espn_id") if away_doc.exists else None

                    # Generar matchId: id1_id2_fecha
                    if home_id and away_id:
                        date_short = match_date[:10]  # YYYY-MM-DD
                        match_id = f"{home_id}_{away_id}_{date_short}"
                    else:
                        match_id = None

                    # MatchKey con venue + fecha (puede servir para otras cosas)
                    match_key = f"{venue}_{match_date[:10]}" if venue else None

                    match_data = {
                        "homeTeam": home_name,
                        "awayTeam": away_name,
                        "datetime": match_date,
                        "matchKey": match_key,
                        "venue": venue,
                        "espnHomeId": home_id,
                        "espnAwayId": away_id,
                        "matchId": match_id
                    }

                    matches_list.append(match_data)

                matchweek_doc = {
                    "id": matchweek_id,
                    "league": league_id,
                    "season": season_year,
                    "matchday": matchday,
                    "matches": matches_list,
                    "createdFrom": "fallback_api"
                }

                db.collection("sports").document("football").collection("matchweeks").document(matchweek_id).set(matchweek_doc, merge=True)
                print(f"📆 Matchweek saved: {matchweek_id}")

        return "✅ Matchweeks actualizadas con matchId (id1_id2_fecha)", 200

    except Exception as e:
        print(f"❌ Error in matchweek fallback: {e}")
        return str(e), 500


@app.route("/update_matches", methods=["GET"])
def update_matches(request):
    import datetime
    import requests

    LEAGUES = [
        "esp.1", "eng.1", "ger.1", "fra.1", "ita.1",
        "uefa.champions", "uefa.europa", "fifa.world", "uefa.euro",
        "conmebol.copa", "caf.nations"
    ]

    today = datetime.date.today()
    days_to_check = [-1, 0, 1, 2]  # Día anterior, actual y 2 siguientes

    for league in LEAGUES:
        print(f"📅 Checking matches for {league}...")
        for offset in days_to_check:
            check_date = today + datetime.timedelta(days=offset)
            date_str = check_date.strftime("%Y%m%d")

            url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/scoreboard?dates={date_str}"
            response = requests.get(url)

            if response.status_code != 200:
                print(f"⚠️ Error fetching data for {league} on {date_str}")
                continue

            data = response.json()
            events = data.get("events", [])

            for event in events:
                try:
                    competition = event["competitions"][0]
                    home = [c for c in competition["competitors"] if c["homeAway"] == "home"][0]
                    away = [c for c in competition["competitors"] if c["homeAway"] == "away"][0]

                    game_id = event["id"]
                    date = event["date"][:10]
                    status = competition["status"]["type"]["description"]
                    finished = competition["status"]["type"]["completed"]
                    venue = competition.get("venue", {}).get("fullName")
                    match_key = f"{venue}_{date}" if venue else None

                    def extract_stats(comp):
                        stats_raw = comp.get("statistics", [])
                        return {s["name"]: s.get("displayValue", s.get("value", None)) for s in stats_raw}

                    stats_home = extract_stats(home)
                    stats_away = extract_stats(away)

                    events_list = []
                    for detail in competition.get("details", []):
                        tipo = detail.get("type", {}).get("text")
                        for jugador in detail.get("athletesInvolved", []):
                            events_list.append({
                                "playerId": jugador.get("id"),
                                "playerName": jugador.get("fullName"),
                                "teamId": detail.get("team", {}).get("id"),
                                "minute": detail.get("clock", {}).get("displayValue"),
                                "type": tipo
                            })

                    match_doc = {
                        "id": game_id,
                        "matchKey": match_key,
                        "date": date,
                        "status": status,
                        "finished": finished,
                        "homeTeam": {
                            "id": home["id"],
                            "name": home["team"]["displayName"],
                            "score": int(home.get("score", 0)),
                            "stats": stats_home
                        },
                        "awayTeam": {
                            "id": away["id"],
                            "name": away["team"]["displayName"],
                            "score": int(away.get("score", 0)),
                            "stats": stats_away
                        },
                        "events": events_list
                    }

                    db.collection("sports").document("football").collection("matches").document(game_id).set(match_doc, merge=True)
                    print(f"✅ Match saved: {game_id} ({home['team']['displayName']} vs {away['team']['displayName']})")

                except Exception as e:
                    print(f"❌ Error parsing match in {league} on {date_str}: {e}")
                    continue

    return "✅ Matches updated successfully", 200



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
