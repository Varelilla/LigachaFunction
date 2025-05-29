from google.cloud import bigquery
import os

# Configurar las credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery-key.json"
client = bigquery.Client(project="ligacha-4041c")
dataset_id = "ligacha_data"

def create_table(table_id, schema):
    full_id = f"{client.project}.{dataset_id}.{table_id}"
    table = bigquery.Table(full_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"✅ Tabla creada: {full_id}")

def create_all_tables():
    # 1. match_results
    create_table("match_results", [
        bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sport", "STRING"),
        bigquery.SchemaField("league", "STRING"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("match_date", "DATE"),
        bigquery.SchemaField("home_team", "STRING"),
        bigquery.SchemaField("away_team", "STRING"),
        bigquery.SchemaField("home_score", "INTEGER"),
        bigquery.SchemaField("away_score", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("events", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("minute", "STRING"),
            bigquery.SchemaField("playerId", "STRING"),
            bigquery.SchemaField("playerName", "STRING"),
            bigquery.SchemaField("teamId", "STRING"),
            bigquery.SchemaField("type", "STRING")
        ]),
        bigquery.SchemaField("source", "STRING")
    ])

    # 2. players_stats
    create_table("players_stats", [
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_name", "STRING"),
        bigquery.SchemaField("team", "STRING"),
        bigquery.SchemaField("match_date", "DATE"),
        bigquery.SchemaField("league", "STRING"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("sport", "STRING"),
        bigquery.SchemaField("stats", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("value", "FLOAT")
        ]),
        bigquery.SchemaField("points", "FLOAT"),
        bigquery.SchemaField("source", "STRING")
    ])

    # 3. ended_bets
    create_table("ended_bets", [
        bigquery.SchemaField("bet_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("league_id", "STRING"),
        bigquery.SchemaField("fantasy_group_id", "STRING"),
        bigquery.SchemaField("matchweek_id", "STRING"),
        bigquery.SchemaField("bet_type", "STRING"),
        bigquery.SchemaField("selected_team_id", "STRING"),
        bigquery.SchemaField("selected_team_name", "STRING"),
        bigquery.SchemaField("result_status", "STRING"),
        bigquery.SchemaField("own_team", "STRING"),
        bigquery.SchemaField("own_team_score", "INTEGER"),
        bigquery.SchemaField("rival_team", "STRING"),
        bigquery.SchemaField("rival_team_score", "INTEGER"),
        bigquery.SchemaField("timestamp", "TIMESTAMP")
    ])

    # 4. fantasy_groups_info (finalizados)
    create_table("fantasy_groups_info", [
        bigquery.SchemaField("group_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("created_by", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("associated_league_id", "STRING"),
        bigquery.SchemaField("matchweek_id", "STRING"),
        bigquery.SchemaField("pot_amount", "FLOAT"),
        bigquery.SchemaField("pot_distributed", "BOOLEAN"),
        bigquery.SchemaField("pot_percent", "RECORD", fields=[
            bigquery.SchemaField("first", "FLOAT"),
            bigquery.SchemaField("second", "FLOAT"),
            bigquery.SchemaField("third", "FLOAT")
        ]),
        bigquery.SchemaField("prizes", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("userId", "STRING"),
            bigquery.SchemaField("amount", "FLOAT")
        ]),
        bigquery.SchemaField("rankings", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("userId", "STRING"),
            bigquery.SchemaField("points", "INTEGER"),
            bigquery.SchemaField("gf", "INTEGER"),
            bigquery.SchemaField("ga", "INTEGER")
        ])
    ])

    # 5. ended_matchweeks
    create_table("ended_matchweeks", [
        bigquery.SchemaField("matchweek_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("pot", "FLOAT"),
        bigquery.SchemaField("teams", "STRING", mode="REPEATED")
    ])

    # 6. transactions
    create_table("transactions", [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("fantasy_group_id", "STRING"),
        bigquery.SchemaField("amount", "FLOAT"),
        bigquery.SchemaField("method", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP")
    ])

if __name__ == "__main__":
    create_all_tables()
