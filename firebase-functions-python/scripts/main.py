from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, firestore
import requests
import datetime
import pandas as pd

app = Flask(__name__)  # Necesario para una función HTTP en Cloud Functions

# Inicializar Firebase solo si no está ya inicializado
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate(r"C:\Users\varel\Downloads\firebase-functions-python\ligacha-4041c-firebase-adminsdk-fbsvc-0bd7586223.json")  # Cambia por la ruta real
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Función para actualizar resultados
def update_results():
    try:
        today_date = datetime.date.today().strftime("%Y-%m-%d")

        # API request to get match results
        url = "https://api.football-data.org/v4/competitions/PL/matches"
        headers = {"X-Auth-Token": "789235b2ab894e28b3f32e731a416675"}  # Reemplaza con tu API Key real

        print(f"📡 Fetching match results for: {today_date}")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            print(f"✅ API Response: {data}")

            matches_today = [
                match for match in data.get("matches", []) if match.get("utcDate", "").startswith(today_date)
            ]

            if not matches_today:
                print(f"⚠️ No matches found for today ({today_date}).")
                return "No matches found", 200

            df = pd.DataFrame(matches_today)
            print(f"📊 Filtered matches: {df}")

            for index, row in df.iterrows():
                match_id = f"match_{row['id']}"
                match_status = row["status"]
                local_goals = row.get("score", {}).get("fullTime", {}).get("home", None)
                visitant_goals = row.get("score", {}).get("fullTime", {}).get("away", None)

                local_result = ""
                visitant_result = ""
                if match_status == "FINISHED":
                    if local_goals > visitant_goals:
                        local_result = "victory"
                        visitant_result = "defeat"
                    elif local_goals < visitant_goals:
                        local_result = "defeat"
                        visitant_result = "victory"
                    else:
                        local_result = "draw"
                        visitant_result = "draw"

                match_data = {
                    "local": row["homeTeam"]["name"],
                    "local_goals": local_goals if local_goals is not None else 0,
                    "local_result": local_result,
                    "visitant": row["awayTeam"]["name"],
                    "visitant_goals": visitant_goals if visitant_goals is not None else 0,
                    "visitant_result": visitant_result,
                    "matchweek_id": None,
                    "players": {},
                }

                db.collection("result").document(match_id).set(match_data, merge=True)

                print(f"🔥 Updated Firestore: {match_data}")

            print("✅ Results successfully updated in Firestore.")
            return "Results updated", 200

        else:
            print(f"⚠️ API Error: {response.status_code} - {response.text}")
            return f"API Error: {response.text}", response.status_code

    except Exception as e:
        print(f"❌ Error: {e}")
        return str(e), 500

# Endpoint HTTP para ejecutar la función desde Cloud Scheduler
@app.route("/", methods=["GET"])
def trigger_update():
    return update_results()

# Ejecutar localmente si el script corre manualmente
if __name__ == "__main__":
    app.run(port=8080)
