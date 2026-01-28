import connexion
from connexion import NoContent
import httpx
import os
import json
from datetime import datetime

STORAGE_URL = "http://localhost:8089"

def _save(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def report_server_health_readings(body):
    status = 201

    for reading in body["readings"]:
        payload = {
            "server_id": body["server_id"],
            "sent_timestamp": body["sent_timestamp"],
            "batch_id": body["batch_id"],
            "server_region": body["server_region"],

            "server_location": reading.get("server_location"),
            "active_players": reading["active_players"],
            "cpu_usage": reading["cpu_usage"],
            "ram_usage": reading["ram_usage"],
            "recorded_timestamp": reading["recorded_timestamp"],
        }

        r = httpx.post(f"{STORAGE_URL}/events/server-health", json=payload)
        status = r.status_code
        if status >= 400:
            break

    return NoContent, status

def report_player_telemetry_event(body):
    status = 201

    for event in body["events"]:
        payload = {
            "server_id": body["server_id"],
            "sent_timestamp": body["sent_timestamp"],
            "batch_id": body["batch_id"],
            "server_region": body.get("server_region"),

            "player_id": event["player_id"],
            "event_timestamp": event["event_timestamp"],
            "player_ping": event["player_ping"],
            "player_level": event.get("player_level"),
            "action": event.get("action"),
        }

        r = httpx.post(f"{STORAGE_URL}/events/player-telemetry", json=payload)
        status = r.status_code
        if status >= 400:
            break

    return NoContent, status

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("game_server_api.yml",
            strict_validation=True,
            validate_responses=True)


if __name__ == "__main__":
    app.run(port=8088)