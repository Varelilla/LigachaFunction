import os
from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, firestore, storage
import requests
import re
import time
import datetime
import pandas as pd
from google.api_core.retry import Retry
import json
from firebase_functions import firestore_fn
import stripe

# Clave secreta de Stripe
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "pk_test_51RSgkbDGlAqrXKn8BfEFUlbVoKJCr4mv3gazPyEuBXet17H4NfB0qxwXMa1k2tNOIWeoBHRI5Deu2Ifd6iBQ8dPu00oO7VGhVY")  # usa env si puedes


app = Flask(__name__)

# Inicializar Firebase
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred, {
        "storageBucket": "ligacha-4041c.firebasestorage.app"
    })


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

@app.route("/football_update_teams", methods=["GET"])
def football_update_teams(request):
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


@app.route("/football_update_players", methods=["GET"])
def football_update_players(request):
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

    
@app.route("/football_update_leagues", methods=["GET"])
def football_update_leagues(request):
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

@app.route("/football_update_standings", methods=["GET"])
def football_update_standings(request):
    LEAGUES = [
        "esp.1", "eng.1", "ger.1", "fra.1", "ita.1",
        "uefa.champions", "uefa.europa", "fifa.world",
        "uefa.euro", "conmebol.copa", "caf.caf"
    ]

    for league in LEAGUES:
        try:
            url = f"https://site.api.espn.com/apis/v2/sports/soccer/{league}/standings"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"❌ Error fetching standings for {league}")
                continue

            data = response.json()
            children = data.get("children", [])
            for group in children:
                entries = group.get("standings", {}).get("entries", [])
                for entry in entries:
                    team = entry.get("team", {})
                    team_id = team.get("id")
                    stats_raw = entry.get("stats", [])
                    stats_clean = {s.get("name"): s.get("value") for s in stats_raw if s.get("name")}
                    doc = {
                        "teamId": team_id,
                        "teamName": team.get("displayName"),
                        "abbreviation": team.get("abbreviation"),
                        "league": league,
                        "stats": stats_clean
                    }
                    doc_ref = db.collection("sports").document("football").collection("standings").document(f"{league}_{team_id}")
                    doc_ref.set(doc, merge=True)
                    print(f"✅ Standings updated: {doc['teamName']} ({league})")

        except Exception as e:
            print(f"❌ Error processing {league}: {e}")

    return "✅ Football standings updated", 200


@app.route("/football_update_matchweeks_fallback", methods=["GET"])
def football_update_matchweeks_fallback(request):
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


@app.route("/football_update_matches", methods=["GET"])
def football_update_matches(request):
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
                        "league": league,
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

@app.route("/basketball_update_teams", methods=["GET"])
def basketball_update_teams(request):
    try:
        LEAGUE_CODE = "nba"  # Para ESPN
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{LEAGUE_CODE}/teams"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"❌ Error fetching NBA teams (status {response.status_code})")
            return f"Error fetching NBA teams", 500

        data = response.json()
        teams = data["sports"][0]["leagues"][0].get("teams", [])

        for team_data in teams:
            team = team_data["team"]
            team_id = team["id"]

            # Extraer logos
            logos = team.get("logos", [])
            logo = next((l["href"] for l in logos if "dark" not in l["href"]), None)
            logo_dark = next((l["href"] for l in logos if "dark" in l["href"]), None)

            team_doc = {
                "id": team_id,
                "slug": team.get("slug"),
                "name": team.get("name"),
                "nickname": team.get("nickname"),
                "displayName": team.get("displayName"),
                "shortName": team.get("shortDisplayName"),
                "abbreviation": team.get("abbreviation"),
                "location": team.get("location"),
                "color": team.get("color"),
                "alternateColor": team.get("alternateColor"),
                "logo": logo,
                "logoDark": logo_dark,
                "isActive": team.get("isActive", True),
                "sport": "basketball",
                "league": "nba"
            }

            doc_ref = db.collection("sports").document("basketball").collection("teams").document(str(team_id))
            doc_ref.set(team_doc, merge=True)
            print(f"✅ NBA Team saved: {team_doc['displayName']}")

        return "✅ NBA Teams updated", 200

    except Exception as e:
        print(f"❌ Error updating NBA teams: {e}")
        return str(e), 500
    
@app.route("/basketball_update_players", methods=["GET"])
def basketball_update_players(request):
    try:
        teams_ref = db.collection("sports").document("basketball").collection("teams")
        page_size = 50
        last_doc = None

        while True:
            query = teams_ref.limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)

            teams = list(query.stream())
            if not teams:
                break

            for team_doc in teams:
                team = team_doc.to_dict()
                team_id = team.get("id")
                team_name = team.get("name")
                print(f"🔍 Getting players for {team_name} (ID {team_id})")

                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/roster"
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"❌ Error fetching roster for team {team_id}")
                    continue

                data = response.json()
                players = data.get("athletes", [])
                print(f"👥 {len(players)} players found for {team_name}")

                for player in players:
                    player_id = str(player.get("id"))

                    player_doc = {
                        "id": player_id,
                        "fullName": player.get("fullName"),
                        "shortName": player.get("shortName"),
                        "jersey": player.get("jersey"),
                        "position": player.get("position", {}).get("abbreviation"),
                        "teamId": team_id,
                        "teamName": team_name,
                        "age": player.get("age"),
                        "height": player.get("height"),
                        "weight": player.get("weight"),
                        "headshot": player.get("headshot", {}).get("href"),
                        "sport": "basketball",
                        "league": "nba"
                    }

                    doc_ref = db.collection("sports").document("basketball").collection("players").document(player_id)
                    doc_ref.set(player_doc, merge=True)

                    print(f"✅ Player updated: {player_doc['fullName']} ({player_id})")

            last_doc = teams[-1]

        return "✅ All NBA players updated", 200

    except Exception as e:
        print(f"❌ Error updating basketball players: {e}")
        return str(e), 500

@app.route("/basketball_update_standings", methods=["GET"])
def basketball_update_standings(request):
    try:
        url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
        response = requests.get(url)
        if response.status_code != 200:
            return "❌ Error fetching basketball standings", 500

        data = response.json()
        children = data.get("children", [])
        for group in children:
            entries = group.get("standings", {}).get("entries", [])
            for entry in entries:
                team = entry.get("team", {})
                team_id = team.get("id")
                stats_raw = entry.get("stats", [])
                stats_clean = {s.get("name"): s.get("value") for s in stats_raw if s.get("name")}
                doc = {
                    "teamId": team_id,
                    "teamName": team.get("displayName"),
                    "abbreviation": team.get("abbreviation"),
                    "league": "nba",
                    "stats": stats_clean
                }
                doc_ref = db.collection("sports").document("basketball").collection("standings").document(team_id)
                doc_ref.set(doc, merge=True)
                print(f"✅ Basketball standings updated: {doc['teamName']}")

        return "✅ Basketball standings updated", 200

    except Exception as e:
        print(f"❌ Error updating basketball standings: {e}")
        return str(e), 500
    
@app.route("/basketball_update_matches", methods=["GET"])
def basketball_update_matches(request):
    try:
        from datetime import datetime, timedelta

        LEAGUE = "nba"
        SPORT = "basketball"
        today = datetime.today().date()
        dates_to_check = [(today + timedelta(days=offset)).strftime("%Y%m%d") for offset in [-1, 0, 1, 2]]

        for date_str in dates_to_check:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT}/{LEAGUE}/scoreboard?dates={date_str}"
            response = requests.get(url)

            if response.status_code != 200:
                print(f"⚠️ Error fetching scoreboard for {date_str}")
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

                    # Estadísticas del equipo (no jugadores)
                    def extract_team_stats(team_obj):
                        return {stat["name"]: stat.get("displayValue", None) for stat in team_obj.get("statistics", [])}

                    stats_home = extract_team_stats(home)
                    stats_away = extract_team_stats(away)

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
                        "sport": "basketball",
                        "league": "nba"
                    }

                    db.collection("sports").document("basketball").collection("matches").document(game_id).set(match_doc, merge=True)
                    print(f"✅ Match saved: {game_id} ({home['team']['displayName']} vs {away['team']['displayName']})")

                except Exception as e:
                    print(f"❌ Error parsing match on {date_str}: {e}")
                    continue

        return "✅ NBA Matches updated successfully", 200

    except Exception as e:
        print(f"❌ Error updating NBA matches: {e}")
        return str(e), 500
    
@app.route("/football_fill_missing_headshots", methods=["GET"])
def football_fill_missing_headshots(request):
    try:
        players_ref = db.collection("sports").document("football").collection("players")
        players = players_ref.where("headshot", "==", None).stream()

        for player_doc in players:
            player = player_doc.to_dict()
            player_id = player["id"]
            full_name = player["fullName"]

            # 1. Buscar en Wikipedia
            search_url = "https://en.wikipedia.org/w/api.php"
            search_params = {
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": full_name
            }
            search_resp = requests.get(search_url, params=search_params).json()
            search_results = search_resp.get("query", {}).get("search", [])

            if not search_results:
                print(f"❌ No Wikipedia entry found for {full_name}")
                continue

            # 2. Obtener thumbnail
            title = search_results[0]["title"]
            page_params = {
                "action": "query",
                "format": "json",
                "prop": "pageimages",
                "titles": title,
                "pithumbsize": 400
            }
            page_resp = requests.get(search_url, params=page_params).json()
            pages = page_resp.get("query", {}).get("pages", {})

            image_url = None
            for page in pages.values():
                image_url = page.get("thumbnail", {}).get("source")

            if not image_url:
                print(f"⚠️ No image for {full_name}")
                continue

            # 3. Descargar y subir imagen
            img_data = requests.get(image_url).content
            bucket = storage.bucket()
            blob = bucket.blob(f"players_photos/{player_id}.jpg")
            blob.upload_from_string(img_data, content_type="image/jpeg")
            blob.make_public()

            photo_url = blob.public_url
            db.collection("sports").document("football").collection("players").document(player_id).update({
                "headshot": photo_url
            })
            print(f"✅ {full_name} → {photo_url}")

        return "✅ Fotos actualizadas para jugadores sin headshot", 200

    except Exception as e:
        print(f"❌ Error actualizando fotos: {e}")
        return str(e), 500

from datetime import datetime,timedelta

API_KEY2 = "6b270684fce3cc5531f1b0bccf83ecc3"
API_URL2 = "https://v3.football.api-sports.io"
HEADERS2 = {
    "x-apisports-key": API_KEY2
}

@app.route("/football_update_players_stats", methods=["GET"])
def football_update_players_stats(request):
    today = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    fixtures_url = f"{API_URL2}/fixtures?date={today}"
    fixtures_res = requests.get(fixtures_url, headers=HEADERS2)
    fixtures = fixtures_res.json().get("response", [])


    ALLOWED_LEAGUE_IDS = [140, 39, 78, 61, 135, 2, 3, 1, 4, 9, 6]
    for fixture in fixtures:
        league_id = fixture["league"]["id"]
        if league_id not in ALLOWED_LEAGUE_IDS:
            continue

        fixture_id = fixture["fixture"]["id"]
        stats_url = f"{API_URL2}/fixtures/players?fixture={fixture_id}"
        stats_res = requests.get(stats_url, headers=HEADERS2)
        if stats_res.status_code != 200:
            continue

        players_data = stats_res.json().get("response", [])
        all_stats = []

        for team in players_data:
            for player in team["players"]:
                stat = player["statistics"][0]
                entry = {
                    "name": player["player"]["name"].lower(),
                    "team": team["team"]["name"].lower(),
                    "goals": stat.get("goals", {}).get("total", 0),
                    "assists": stat.get("goals", {}).get("assists", 0),
                    "shots": stat.get("shots", {}).get("total", 0),
                    "on_target": stat.get("shots", {}).get("on", 0),
                    "passes": stat.get("passes", {}).get("total", 0),
                    "tackles": stat.get("tackles", {}).get("total", 0),
                    "yellow": stat.get("cards", {}).get("yellow", 0),
                    "red": stat.get("cards", {}).get("red", 0),
                    "duels": stat.get("duels", {}).get("total", 0)
                }
                all_stats.append(entry)

        df = pd.DataFrame(all_stats)
        if df.empty:
            continue
        columns_for_points = [
            "goals", "assists", "on_target", "shots", "passes",
            "tackles", "duels", "yellow", "red"
        ]

        for col in columns_for_points:
            if col not in df.columns:
                df[col] = 0

        df[columns_for_points] = df[columns_for_points].fillna(0)

        df["points"] = (
            df["goals"] * 5 +
            df["assists"] * 3 +
            df["on_target"] * 1.5 +
            df["shots"] * 0.5 +
            df["passes"] * 0.05 +
            df["tackles"] * 0.5 +
            df["duels"] * 0.1 -
            df["yellow"] * 1 -
            df["red"] * 3
        ).round(2)
        players_ref = db.collection("sports").document("football").collection("players").stream()
        all_players = [doc for doc in players_ref]

        for _, row in df.iterrows():
            print(f"⚽ {row['name']} ({row['team']}) - {row['points']} puntos")

            player_name_norm = normalize_string(row["name"])


            matched = False
            for doc in all_players:
                player_data = doc.to_dict()
                full_name_norm = normalize_string(player_data.get("fullName", ""))

                if full_name_norm in player_name_norm or player_name_norm in full_name_norm:
                    doc_ref = doc.reference
                    current_data = player_data
                    prev_season_points = current_data.get("points_season", 0)
                    new_match_points = row["points"]
                    doc_ref.update({
                        "points": new_match_points,
                        "points_season": round(prev_season_points + new_match_points, 2)
                    })
                    print(f"✅ Actualizado: {player_data['fullName']} con {new_match_points} puntos.")
                    matched = True
                    break

            if not matched:
                print(f"❌ No se encontró coincidencia para {row['name']}")

        return "Actualización de stats completada", 200

import unicodedata

def normalize_string(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")
    return s.lower()

import random
@app.route("/simulate_season_points", methods=["GET"])
def simulate_season_points(request):
    players_ref = db.collection("sports").document("football").collection("players")
    players = players_ref.stream()

    updates = 0
    for player in players:
        doc_ref = player.reference
        total_points = round(sum(random.uniform(5, 15) for _ in range(16)), 2)
        print(f"🎯 {player.id} -> {total_points} puntos simulados")
        doc_ref.update({"points_season": total_points})
        updates += 1

    return f"✅ Se actualizaron {updates} jugadores con puntuaciones simuladas", 200

from io import BytesIO
def is_valid_image(blob):
    try:
        img_bytes = blob.download_as_bytes()
        Image.open(BytesIO(img_bytes)).verify()
        return True
    except:
        return False
    
bucket = storage.bucket()   
    
@app.route("/fix_broken_photos", methods=["GET"])
def fix_broken_photos(request):
    blobs = bucket.list_blobs(prefix="players_photos/")
    broken = []

    for blob in blobs:
        if not blob.name.endswith(".jpg"):
            continue

        if not is_valid_image(blob):
            print(f"❌ Imagen corrupta: {blob.name}")
            broken.append(blob.name)

    for path in broken:
        player_id = os.path.basename(path).replace(".jpg", "")
        doc = db.collection("sports").document("football").collection("players").document(player_id).get()

        if not doc.exists:
            print(f"⛔ No se encontró jugador con ID {player_id}")
            continue

        full_name = doc.to_dict().get("fullName")
        if not full_name:
            print(f"⚠️ No hay fullName para {player_id}")
            continue

        wiki_name = full_name.replace(" ", "_")
        url = f"https://en.wikipedia.org/wiki/{wiki_name}"

        try:
            html = requests.get(url).text
            start = html.find('<table class="infobox')  # Buscar tabla de infobox
            img_start = html.find('<img src="//upload.', start)
            img_url = html[img_start + 10:html.find('"', img_start + 10)]
            img_url = "https://upload." + img_url

            print(f"📷 Re-descargando imagen desde {img_url}")

            img_data = requests.get(img_url).content
            blob = bucket.blob(f"players_photos/{player_id}.jpg")
            blob.upload_from_string(img_data, content_type="image/jpeg")
            print(f"✅ Imagen de {full_name} reparada.")

        except Exception as e:
            print(f"❌ Error procesando {full_name}: {e}")

    return {"status": "done", "fixed": len(broken)}, 200

# Funcion hecha para crear jornadas falsas para la presentacion de la app
@app.route("/create_fake_matchweeks_40", methods=["GET"])
def create_fake_matchweeks_40(request):

    COLLECTION = db.collection("sports").document("football").collection("matchweeks")

    try:
        leagues_processed = 0
        leagues = set()

        matchweeks = COLLECTION.stream()

        # Obtener última jornada por liga
        latest_by_league = {}
        for doc in matchweeks:
            data = doc.to_dict()
            league = data.get("league")
            matchday = data.get("matchday")

            if league not in latest_by_league or matchday > latest_by_league[league]["matchday"]:
                latest_by_league[league] = {
                    "matchday": matchday,
                    "data": data
                }

        for league, info in latest_by_league.items():
            source_data = info["data"]
            season = source_data["season"]
            fake_id = f"{league}_{season}_40"

            print(f"🎭 Cloning matchweek for {league} -> {fake_id}")

            new_matches = []
            base_date = datetime(2025, 6, 4, 15, 0)  # 4 de junio 15:00
            date_choices = []

            # Generar slots cada 30 minutos entre 15:00 y 21:00 para los días 4 y 5
            for day in [4, 5]:
                for hour in range(15, 21 + 1):
                    for minute in [0, 30]:
                        date_choices.append(datetime(2025, 6, day, hour, minute))

            random.shuffle(date_choices)

            for i, match in enumerate(source_data["matches"]):
                new_datetime = date_choices[i % len(date_choices)].isoformat() + "Z"

                new_match = {
                    **match,
                    "datetime": new_datetime,
                    "matchId": f"{match['espnHomeId']}_{match['espnAwayId']}_{new_datetime[:10]}"
                }

                new_matches.append(new_match)

            new_doc = {
                "id": fake_id,
                "league": league,
                "season": season,
                "matchday": 40,
                "matches": new_matches,
                "createdFrom": "fake_presentation"
            }

            COLLECTION.document(fake_id).set(new_doc, merge=True)
            leagues_processed += 1
            print(f"✅ Fake matchweek created for {league}")

        return f"🎉 {leagues_processed} fake matchweeks creadas con jornada 40", 200

    except Exception as e:
        print(f"❌ Error: {e}")
        return f"Error: {e}", 500


@app.route("/create_stripe_payment_intent", methods=["GET"])
def create_stripe_payment_intent():
    try:
        # Recoge el parámetro "amount" desde la query string
        amount_param = request.args.get("amount")

        if not amount_param:
            return {"error": "Missing amount"}, 400

        try:
            amount = float(amount_param)
        except ValueError:
            return {"error": "Invalid amount"}, 400

        intent = stripe.PaymentIntent.create(
            amount=int(amount * 100),  # céntimos
            currency="eur",
            payment_method_types=["card"]
        )

        return {
            "clientSecret": intent.client_secret
        }, 200

    except Exception as e:
        print(f"❌ Error creando PaymentIntent: {e}")
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
