from google.cloud import bigquery
from datetime import datetime, timedelta
import random
import uuid

client = bigquery.Client(project="ligacha-4041c")

# Utilidades

def random_date(start_year=2010, end_year=2020):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return datetime(year, month, day, hour, minute).isoformat()

# 1. match_results
match_rows = []
for _ in range(500):
    date = random_date()
    match_rows.append({
        "match_id": str(uuid.uuid4()),
        "sport": "football",
        "league": random.choice(["esp.1", "eng.1", "ita.1"]),
        "season": date[:4],
        "match_date": date[:10],
        "home_team": f"Team_{random.randint(1, 50)}",
        "away_team": f"Team_{random.randint(51, 100)}",
        "home_score": random.randint(0, 5),
        "away_score": random.randint(0, 5),
        "status": "Full Time",
        "events": [],
        "source": "fake"
    })
client.insert_rows_json("ligacha-4041c.ligacha_data.match_results", match_rows)

# 2. ended_bets
bets_rows = []
for _ in range(500):
    date = random_date()
    bets_rows.append({
        "bet_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 200)}",
        "league_id": random.choice(["esp.1__2019", "eng.1__2018"]),
        "fantasy_group_id": f"group_{random.randint(1, 100)}",
        "matchweek_id": f"week_{random.randint(1, 38)}",
        "bet_type": random.choice(["roulette", "builder"]),
        "selected_team_id": f"team_{random.randint(1, 50)}",
        "selected_team_name": f"Team {random.randint(1, 50)}",
        "result_status": random.choice(["win", "lose"]),
        "own_team": f"Team {random.randint(1, 50)}",
        "own_team_score": random.randint(0, 5),
        "rival_team": f"Team {random.randint(1, 50)}",
        "rival_team_score": random.randint(0, 5),
        "timestamp": date
    })
client.insert_rows_json("ligacha-4041c.ligacha_data.ended_bets", bets_rows)

# 3. fantasy_groups_info
groups_rows = []
for _ in range(200):
    created = random_date()
    finished = random_date()
    groups_rows.append({
        "group_id": str(uuid.uuid4()),
        "name": f"Group_{random.randint(1, 100)}",
        "created_by": f"user_{random.randint(1, 100)}",
        "created_at": created,
        "finished_at": finished,
        "associated_league_id": random.choice(["esp.1__2019", "eng.1__2018"]),
        "matchweek_id": f"week_{random.randint(1, 38)}",
        "pot_amount": round(random.uniform(10, 300), 2),
        "pot_distributed": True,
        "pot_percent": {
            "first": 50,
            "second": 30,
            "third": 20
        },
        "prizes": [
            {"position": "first", "userId": "user_1", "amount": 150.0},
            {"position": "second", "userId": "user_2", "amount": 90.0},
            {"position": "third", "userId": "user_3", "amount": 60.0}
        ],
        "rankings": [
            {"userId": "user_1", "points": 9, "gf": 12, "ga": 5},
            {"userId": "user_2", "points": 6, "gf": 8, "ga": 7},
            {"userId": "user_3", "points": 3, "gf": 5, "ga": 10}
        ]
    })
client.insert_rows_json("ligacha-4041c.ligacha_data.fantasy_groups_info", groups_rows)

# 4. ended_matchweeks
weeks_rows = []
for _ in range(300):
    weeks_rows.append({
        "matchweek_id": f"week_fake_{random.randint(1, 999)}",
        "created_at": random_date(),
        "pot": round(random.uniform(20, 500), 2),
        "teams": [f"team_{random.randint(1, 50)}" for _ in range(18)]
    })
client.insert_rows_json("ligacha-4041c.ligacha_data.ended_matchweeks", weeks_rows)

# 5. transactions
tx_rows = []
for _ in range(500):
    tx_rows.append({
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 200)}",
        "fantasy_group_id": f"group_{random.randint(1, 100)}",
        "amount": round(random.uniform(1, 200), 2),
        "method": random.choice(["balance", "card", "paypal"]),
        "type": random.choice(["win_roulette", "win_builder", "draw_roulette", "deposit", "withdrawal"]),
        "status": random.choice(["completed", "pending", "failed"]),
        "timestamp": random_date()
    })
client.insert_rows_json("ligacha-4041c.ligacha_data.transactions", tx_rows)

print("✅ Datos falsos insertados en BigQuery correctamente.")