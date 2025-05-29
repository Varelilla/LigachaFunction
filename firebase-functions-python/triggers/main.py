from firebase_functions import firestore_fn
from firebase_admin import initialize_app, firestore
from datetime import datetime

# Inicializar Firebase
initialize_app()
db = firestore.client()

# 🔥 TRIGGER PRINCIPAL
@firestore_fn.on_document_updated(document="sports/football/matches/{matchId}")
def on_match_updated(event, context):
    before = event.data.before.to_dict()
    after = event.data.after.to_dict()

    if not after.get("finished", False):
        return

    if before.get("finished") == after.get("finished"):
        return

    home = after.get("homeTeam", {})
    away = after.get("awayTeam", {})
    league = after.get("league", "")
    date = after.get("date", "")
    finished = after.get("finished", False)

    if home.get("score") == 0 and away.get("score") == 0 and not finished:
        print("⚠️ Partido con resultado 0-0. Ignorando actualización.")
        return

    jornada_num = after.get("matchKey", "").split("_")[-1] if "matchKey" in after else "XX"
    matchweek_id = f"{league}_{date[:4]}_{jornada_num}"
    print(f"🎯 Procesando apuestas para: {matchweek_id}")

    bets_ref = db.collection("bets").where("matchweekId", "==", matchweek_id).where("type", "==", "roulette")
    bets = bets_ref.stream()

    for bet in bets:
        bet_data = bet.to_dict()
        matchweek_id = bet_data["matchweekId"]
        user_id = bet_data["userId"]
        fantasy_group_id = bet_data["fantasyGroupId"]
        bet_id = bet.id

        # ✅ 1. Generar matchId actual
        home_id = home.get("id")
        away_id = away.get("id")
        date_str = after.get("date")
        generated_match_id = f"{home_id}_{away_id}_{date_str}"

        # ✅ 2. Consultar la jornada
        matchweek_ref = db.collection("matchweeks").document(matchweek_id)
        matchweek_doc = matchweek_ref.get()

        if not matchweek_doc.exists:
            print(f"⚠️ Jornada {matchweek_id} no encontrada. Ignorando apuesta {bet_id}.")
            continue

        matchweek_data = matchweek_doc.to_dict()
        match_ids = [m.get("matchId") for m in matchweek_data.get("matches", [])]

        if generated_match_id not in match_ids:
            print(f"⚠️ Partido {generated_match_id} no está en la jornada {matchweek_id}. Ignorando apuesta {bet_id}.")
            continue
        selected_team = bet_data.get("selectedTeam", {})
        user_id = bet_data["userId"]
        fantasy_group_id = bet_data["fantasyGroupId"]
        bet_id = bet.id

        is_home = selected_team.get("id") == home.get("id")
        is_away = selected_team.get("id") == away.get("id")

        if not is_home and not is_away:
            print(f"⚠️ Apuesta {bet_id} no relacionada con el partido.")
            continue

        own_score = home["score"] if is_home else away["score"]
        rival_score = away["score"] if is_home else home["score"]
        own_team = home["name"] if is_home else away["name"]
        rival_team = away["name"] if is_home else home["name"]

        result_payload = {
            "ownTeam": own_team,
            "ownTeamScore": own_score,
            "rivalTeam": rival_team,
            "rivalTeamScore": rival_score
        }

        status = "lost"
        if own_score > rival_score:
            status = "win"
        elif own_score == rival_score:
            status = "lost"  # empate lógico, pero sin puntos

        update_fields = {
            "result": result_payload,
            "status": status
        }

        is_public = fantasy_group_id == "public"
        amount = 5 if is_public else get_group_bet_amount(fantasy_group_id)

        if own_score < rival_score:
            if is_public:
                update_matchweek_pot(matchweek_id, amount)
            else:
                update_group(fantasy_group_id, pot_increase=amount, user_id=user_id)

        elif own_score == rival_score:
            refund = round(amount * 0.4, 2)
            pot_share = amount - refund
            update_balance(user_id, refund, reason="Empate apuesta", match_id=after["id"], bet_id=bet_id)

            if is_public:
                update_matchweek_pot(matchweek_id, pot_share)
            else:
                update_group(fantasy_group_id, pot_increase=pot_share, user_id=user_id)

        else:  # Victoria
            update_fields["winstatus"] = "calculating"
            if is_public:
                update_matchweek_pot(matchweek_id, amount)
            else:
                update_balance(user_id, amount, reason="Victoria apuesta", match_id=after["id"], bet_id=bet_id)
                update_group(fantasy_group_id, update_points=True, user_id=user_id, gf=own_score, ga=rival_score)

        db.collection("bets").document(bet.id).update(update_fields)

    print(f"✅ Apuestas actualizadas para jornada {matchweek_id}")


# 💰 Función auxiliar: actualizar saldo
def update_balance(user_id, amount, reason, match_id=None, bet_id=None):
    user_ref = db.collection("users").document(user_id)
    user_doc = user_ref.get()

    if not user_doc.exists:
        print(f"⚠️ Usuario {user_id} no encontrado.")
        return

    def transaction_update_balance(transaction):
        snapshot = user_ref.get(transaction=transaction)
        current_balance = snapshot.get("saldo", 0)
        new_balance = round(current_balance + amount, 2)
        transaction.update(user_ref, {"saldo": new_balance})

        trans_doc = {
            "userId": user_id,
            "amount": amount,
            "reason": reason,
            "timestamp": datetime.utcnow(),
            "matchId": match_id,
            "betId": bet_id
        }

        db.collection("transactions").add(trans_doc)

    db.run_transaction(transaction_update_balance)
    print(f"💰 Balance actualizado para {user_id}: {'+' if amount > 0 else ''}{amount}")


# 🏆 Función auxiliar: actualizar grupo privado
def update_group(group_id, pot_increase=0, update_points=False, user_id=None, gf=0, ga=0):
    group_ref = db.collection("fantasy_groups").document(group_id)
    group_doc = group_ref.get()

    if not group_doc.exists:
        print(f"⚠️ Grupo {group_id} no encontrado.")
        return

    updates = {}
    group_data = group_doc.to_dict()

    if pot_increase > 0:
        current_pot = group_data.get("totalPot", 0)
        updates["totalPot"] = round(current_pot + pot_increase, 2)

    if update_points and user_id:
        rankings = group_data.get("rankings", [])
        found = False

        for player in rankings:
            if player["userId"] == user_id:
                player["points"] += 3
                player["gf"] += gf
                player["ga"] += ga
                found = True
                break

        if not found:
            rankings.append({
                "userId": user_id,
                "points": 3,
                "gf": gf,
                "ga": ga
            })

        updates["rankings"] = rankings

    if updates:
        group_ref.update(updates)
        print(f"🏆 Grupo {group_id} actualizado.")


# 🪙 Función auxiliar: actualizar pot de jornada pública
def update_matchweek_pot(matchweek_id, amount):
    pot_ref = db.collection("matchweek_pots").document(matchweek_id)
    pot_doc = pot_ref.get()

    if pot_doc.exists:
        current = pot_doc.to_dict().get("pot", 0)
        pot_ref.update({"pot": round(current + amount, 2)})
    else:
        pot_ref.set({"pot": round(amount, 2)})

    print(f"🪙 Pot de {matchweek_id} actualizado: +{amount}€")


# 🔄 Función auxiliar: obtener apuesta base en grupo
def get_group_bet_amount(group_id):
    group_ref = db.collection("fantasy_groups").document(group_id)
    group_doc = group_ref.get()

    if not group_doc.exists:
        print(f"⚠️ Grupo {group_id} no encontrado. Usando valor por defecto.")
        return 5

    pot_amount = group_doc.to_dict().get("potAmount", 5)

    try:
        return float(pot_amount)
    except (ValueError, TypeError):
        print(f"⚠️ potAmount inválido en grupo {group_id}. Usando 5€ por defecto.")
        return 5
