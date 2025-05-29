from firebase_functions import scheduler_fn, https_fn
import stripe
from firebase_admin import initialize_app, firestore
from google.cloud import bigquery
import os
import json
import pandas as pd
import datetime
import os
import firebase_admin
from firebase_admin import credentials

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "pk_test_51RSgkbDGlAqrXKn8BfEFUlbVoKJCr4mv3gazPyEuBXet17H4NfB0qxwXMa1k2tNOIWeoBHRI5Deu2Ifd6iBQ8dPu00oO7VGhVY")

EXPORT_VERSION = "1.1"
# Inicia Firebase Admin solo una vez
cred = credentials.Certificate("bigquery-key.json")
firebase_admin.initialize_app(cred)
# BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery-key.json"
client = bigquery.Client(project="ligacha-4041c")

db = firestore.client()

def clean_timestamp(ts):
    return ts.isoformat() if ts else None

@scheduler_fn.on_schedule(schedule="30 16 * * 0")
def sync_match_results(event: scheduler_fn.ScheduledEvent):
    try:
        matches_ref = db.collection("sports").document("football").collection("matches")
        matches = matches_ref.where("finished", "==", True).stream()

        table_id = "ligacha-4041c.ligacha_data.match_results"

        rows = []
        for match in matches:
            data = match.to_dict()
            rows.append({
                "match_id": data.get("id"),
                "sport": "football",
                "league": data.get("league"),
                "season": str(data.get("date")[:4]) if data.get("date") else "unknown",
                "match_date": data.get("date"),
                "home_team": data.get("homeTeam", {}).get("name"),
                "away_team": data.get("awayTeam", {}).get("name"),
                "home_score": data.get("homeTeam", {}).get("score"),
                "away_score": data.get("awayTeam", {}).get("score"),
                "status": data.get("status"),
                "events": data.get("events", []),
                "source": "firebase"
            })

        if not rows:
            print("No hay partidos finalizados para subir.")
            return

        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print(f"❌ Errores al insertar: {json.dumps(errors)}")
            return

        print(f"✅ {len(rows)} partidos subidos a BigQuery")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

@scheduler_fn.on_schedule(schedule="0 17 * * 0")
def sync_ended_bets(event: scheduler_fn.ScheduledEvent):
    try:
        bets_ref = db.collection("bets").where("type", "==", "roulette")
        bets = bets_ref.stream()

        rows = []
        to_update = []
        for bet in bets:
            data = bet.to_dict()
            version = data.get("exportedToBigQuery")
            if version == EXPORT_VERSION:
                continue
            if not data.get("result"): continue

            row = {
                "bet_id": bet.id,
                "user_id": data.get("userId"),
                "league_id": data.get("associatedLeagueId"),
                "fantasy_group_id": data.get("fantasyGroupId"),
                "matchweek_id": data.get("matchweekId"),
                "bet_type": data.get("type"),
                "selected_team_id": data.get("selectedTeam", {}).get("id"),
                "selected_team_name": data.get("selectedTeam", {}).get("name"),
                "result_status": data.get("selectedTeam", {}).get("status"),
                "own_team": data.get("result", {}).get("ownTeam"),
                "own_team_score": data.get("result", {}).get("ownTeamScore"),
                "rival_team": data.get("result", {}).get("rivalTeam"),
                "rival_team_score": data.get("result", {}).get("rivalTeamScore"),
                "timestamp": clean_timestamp(data.get("timestamp"))
            }
            rows.append(row)
            to_update.append(bet.reference)

        table_id = "ligacha-4041c.ligacha_data.ended_bets"
        if rows:
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                print(json.dumps(errors))
                return
            for ref in to_update:
                ref.update({"exportedToBigQuery": EXPORT_VERSION})

        print(f"✅ {len(rows)} apuestas subidas.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

@scheduler_fn.on_schedule(schedule="30 17 * * 0")
def sync_fantasy_groups_info(event: scheduler_fn.ScheduledEvent):
    try:
        groups_ref = db.collection("fantasy_groups").where("potDistributed", "==", True)
        groups = groups_ref.stream()

        rows = []
        to_update = []
        for group in groups:
            data = group.to_dict()
            version = data.get("exportedToBigQuery")
            if version == EXPORT_VERSION:
                continue

            row = {
                "group_id": group.id,
                "name": data.get("name"),
                "created_by": data.get("createdBy"),
                "created_at": clean_timestamp(data.get("createdAt")),
                "finished_at": clean_timestamp(data.get("finishedAt")),
                "associated_league_id": data.get("associatedLeagueId"),
                "matchweek_id": data.get("matchweekId"),
                "pot_amount": data.get("potAmount"),
                "pot_distributed": data.get("potDistributed"),
                "pot_percent": data.get("potPercent"),
                "prizes": data.get("prizes", []),
                "rankings": data.get("rankings", [])
            }
            rows.append(row)
            to_update.append(group.reference)

        table_id = "ligacha-4041c.ligacha_data.fantasy_groups_info"
        if rows:
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                print(json.dumps(errors))
                return
            for ref in to_update:
                ref.update({"exportedToBigQuery": EXPORT_VERSION})

        print(f"✅ {len(rows)} grupos subidos.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

@scheduler_fn.on_schedule(schedule="0 18 * * 0")
def sync_ended_matchweeks(event: scheduler_fn.ScheduledEvent):
    try:
        ended_ref = db.collection("ended_matchweeks")
        weeks = ended_ref.stream()

        rows = []
        to_update = []
        for week in weeks:
            data = week.to_dict()
            version = data.get("exportedToBigQuery")
            if version == EXPORT_VERSION:
                continue

            row = {
                "matchweek_id": data.get("matchweekId"),
                "created_at": clean_timestamp(data.get("createdAt")),
                "pot": data.get("pot"),
                "teams": data.get("teams", [])
            }
            rows.append(row)
            to_update.append(week.reference)

        table_id = "ligacha-4041c.ligacha_data.ended_matchweeks"
        if rows:
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                print(json.dumps(errors))
                return
            for ref in to_update:
                ref.update({"exportedToBigQuery": EXPORT_VERSION})

        print(f"✅ {len(rows)} jornadas subidas.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

@scheduler_fn.on_schedule(schedule="30 18 * * 0")
def sync_transactions(event: scheduler_fn.ScheduledEvent):
    try:
        tx_ref = db.collection("transactions")
        txs = tx_ref.stream()

        rows = []
        to_update = []
        for tx in txs:
            data = tx.to_dict()
            version = data.get("exportedToBigQuery")
            if version == EXPORT_VERSION:
                continue

            row = {
                "transaction_id": tx.id,
                "user_id": data.get("userId"),
                "fantasy_group_id": data.get("fantasyGroupId"),
                "amount": data.get("amount"),
                "method": data.get("method"),
                "type": data.get("type"),
                "status": data.get("status"),
                "timestamp": clean_timestamp(data.get("timestamp"))
            }
            rows.append(row)
            to_update.append(tx.reference)

        table_id = "ligacha-4041c.ligacha_data.transactions"
        if rows:
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                print(json.dumps(errors))
                return
            for ref in to_update:
                ref.update({"exportedToBigQuery": EXPORT_VERSION})

        print(f"✅ {len(rows)} transacciones subidas.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

LEAGUES = ["esp.1", "eng.1", "ita.1", "ger.1", "fra.1"]
now = datetime.datetime.utcnow()
CURRENT_SEASON = str(now.year - 1 if now.month <= 6 else now.year)
DRAW_THRESHOLD = 0.7
STD_DEV_THRESHOLD = 0.2

@scheduler_fn.on_schedule(schedule="0 12 * * *")
def generate_matchweek_offers(event: scheduler_fn.ScheduledEvent):
    try:
        created_offers = []
        for league in LEAGUES:
            prefix = f"{league}_{CURRENT_SEASON}_"
            matchweeks_ref = db.collection("matchweeks").where("id", ">=", prefix).stream()
            latest_week = None
            highest_number = -1
            for mw in matchweeks_ref:
                data = mw.to_dict()
                mw_id = data.get("id")
                if not mw_id:
                    continue
                parts = mw_id.split("_")
                if len(parts) == 3 and parts[0] == league and parts[1] == CURRENT_SEASON:
                    try:
                        number = int(parts[2])
                        if number > highest_number:
                            latest_week = data
                            highest_number = number
                    except:
                        continue
            if not latest_week:
                continue
            matchweek_id = latest_week["id"]
            matches = latest_week.get("matches", [])
            draw_ratios = []
            for match in matches:
                team1 = match.get("espnHomeId")
                team2 = match.get("espnAwayId")
                if not team1 or not team2:
                    continue
                query = f'''
                    SELECT
                      COUNT(*) AS total,
                      SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) AS draws
                    FROM `ligacha_data.match_results`
                    WHERE ((home_team = @team1 AND away_team = @team2)
                           OR (home_team = @team2 AND away_team = @team1))
                      AND league = @league
                '''
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("team1", "STRING", str(team1)),
                        bigquery.ScalarQueryParameter("team2", "STRING", str(team2)),
                        bigquery.ScalarQueryParameter("league", "STRING", league)
                    ]
                )
                result = client.query(query, job_config=job_config).to_dataframe()
                if not result.empty and result.loc[0, "total"] > 0:
                    ratio = result.loc[0, "draws"] / result.loc[0, "total"]
                    draw_ratios.append(ratio)
            if not draw_ratios:
                continue
            df = pd.DataFrame(draw_ratios, columns=["draw_ratio"])
            avg = df["draw_ratio"].mean()
            std = df["draw_ratio"].std()
            if avg > DRAW_THRESHOLD and std < STD_DEV_THRESHOLD:
                offer = {
                    "matchweek_id": matchweek_id,
                    "type": "draw_bonus",
                    "description": "¡Empate esta jornada con multiplicador especial x1.5!",
                    "draw_ratio": round(avg, 3),
                    "std_dev": round(std, 3),
                    "created_at": firestore.SERVER_TIMESTAMP
                }
                offer_id = f"{matchweek_id}_draw_offer"
                db.collection("offers").document(offer_id).set(offer)
                created_offers.append(offer_id)
        if created_offers:
            print(f"✅ Ofertas creadas: {created_offers}")
        else:
            print("No se creó ninguna oferta.")
    except Exception as e:
        print(f"❌ Error: {str(e)}")

@https_fn.on_call()
def create_stripe_payment_intent(req: https_fn.CallableRequest) -> dict:
    try:
        # ✅ Opcional: Verifica que el usuario esté autenticado
        if not req.auth:
            raise https_fn.HttpsError("unauthenticated", "Usuario no autenticado")

        amount = req.data.get("amount")
        if amount is None or not isinstance(amount, (int, float)):
            raise https_fn.HttpsError("invalid-argument", "Cantidad no válida")

        intent = stripe.PaymentIntent.create(
            amount=int(amount * 100),
            currency="eur",
            payment_method_types=["card"],
            metadata={
                "user_id": req.auth.uid  # opcional para trazabilidad
            }
        )

        return {
            "clientSecret": intent.client_secret
        }

    except stripe.error.StripeError as e:
        raise https_fn.HttpsError("internal", f"Stripe error: {str(e)}")
    except Exception as e:
        raise https_fn.HttpsError("internal", str(e))
