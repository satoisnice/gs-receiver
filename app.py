import connexion
from connexion import NoContent
import httpx
import os
import json
from datetime import datetime
import time
import yaml
import logging
import logging.config

# STORAGE_URL = "http://localhost:8089"

with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("basicLogger")


def add_trace_id():
    return time.time_ns()


def report_server_health_readings(body):
    status = 201
    trace_id = add_trace_id()
    logger.info("Received event server_health with a trace id of %s", trace_id)

    for reading in body["readings"]:
        payload = {
            "trace_id": trace_id,

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
        # print("SENDING TO STORAGE:", payload)

        # r = httpx.post(f"{STORAGE_URL}/events/server-health", json=payload)
        r = httpx.post(
            app_config["events"]["server_health"]["url"],
            json=payload
        )
        status = r.status_code

        logger.info(
            "Response for event server_health (id: %s) has status %s",
            trace_id,
            status
        )

        if status >= 400:
            break

    return NoContent, status

def report_player_telemetry_event(body):
    status = 201
    trace_id = add_trace_id()
    logger.info("Received event player_telemetry with a trace id of %s", trace_id)

    for event in body["events"]:
        payload = {
            "trace_id": trace_id,

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
        # print("SENDING TO STORAGE:", payload)

        # r = httpx.post(f"{STORAGE_URL}/events/player-telemetry", json=payload)
        r = httpx.post(
            app_config["events"]["player_telemetry"]["url"],
            json=payload
        )
        status = r.status_code

        logger.info(
            "Response for event player_telemetry (id: %s) has status %s",
            trace_id,
            status
        )
        logger.info("Storage response body: %s", r.text)


        if status >= 400:
            break

    return NoContent, status

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("game_server_api.yml",
            strict_validation=True,
            validate_responses=True)


if __name__ == "__main__":
    app.run(port=8088)