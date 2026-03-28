"""
ArduPilot .bin log parser — Fly.io microservice.
Receives a .bin file via POST /parse, returns structured JSON
using pymavlink.
"""

import io
import json
import os
import tempfile

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

PARSER_SECRET = os.environ.get("PARSER_SECRET", "")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/parse", methods=["POST"])
def parse():
    # ── Auth ──
    if PARSER_SECRET:
        token = request.headers.get("X-Parser-Secret", "")
        if token != PARSER_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    # ── File ──
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    uploaded = request.files["file"]
    if not uploaded.filename:
        return jsonify({"error": "Empty filename"}), 400

    # pymavlink needs a real file path
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tmp:
        uploaded.save(tmp)
        tmp_path = tmp.name

    try:
        result = _parse_bin(tmp_path)
        return Response(
            json.dumps(result, ensure_ascii=False),
            mimetype="application/json",
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        os.unlink(tmp_path)


def _parse_bin(path: str) -> dict:
    from pymavlink import mavutil

    mlog = mavutil.mavlink_connection(path, dialect="ardupilotmega")

    gps_list = []
    battery_list = []
    attitude_list = []
    modes_list = []
    messages_list = []
    params = {}
    vehicle_type = "ArduPilot"

    while True:
        msg = mlog.recv_match(blocking=False)
        if msg is None:
            break

        msg_type = msg.get_type()

        if msg_type == "GPS":
            try:
                gps_list.append({
                    "time_ms": int(getattr(msg, "TimeUS", 0) / 1000),
                    "lat": msg.Lat,
                    "lng": msg.Lng,
                    "alt": msg.Alt,
                    "spd": msg.Spd,
                    "nSat": getattr(msg, "NSats", getattr(msg, "nSat", 0)),
                })
            except Exception:
                pass

        elif msg_type in ("BAT", "BATT"):
            try:
                battery_list.append({
                    "time_ms": int(getattr(msg, "TimeUS", 0) / 1000),
                    "volt": msg.Volt,
                    "curr": getattr(msg, "Curr", None),
                    "remaining": getattr(msg, "CurrTot", getattr(msg, "EnrgTot", None)),
                })
            except Exception:
                pass

        elif msg_type == "ATT":
            try:
                attitude_list.append({
                    "time_ms": int(getattr(msg, "TimeUS", 0) / 1000),
                    "pitch": msg.Pitch,
                    "roll": msg.Roll,
                    "yaw": msg.Yaw,
                })
            except Exception:
                pass

        elif msg_type == "MODE":
            try:
                mode_name = getattr(msg, "Mode", str(getattr(msg, "ModeNum", "?")))
                modes_list.append({
                    "time_ms": int(getattr(msg, "TimeUS", 0) / 1000),
                    "mode": str(mode_name),
                })
            except Exception:
                pass

        elif msg_type == "MSG":
            try:
                messages_list.append({
                    "time_ms": int(getattr(msg, "TimeUS", 0) / 1000),
                    "text": msg.Message,
                })
            except Exception:
                pass

        elif msg_type == "PARM":
            try:
                params[msg.Name] = msg.Value
            except Exception:
                pass

    # Detect vehicle type from params or messages
    for m in messages_list:
        txt = m.get("text", "").lower()
        if "arducopter" in txt:
            vehicle_type = "ArduCopter"
            break
        elif "arduplane" in txt:
            vehicle_type = "ArduPlane"
            break
        elif "ardurover" in txt:
            vehicle_type = "ArduRover"
            break
        elif "ardusub" in txt:
            vehicle_type = "ArduSub"
            break

    return {
        "gps": gps_list,
        "battery": battery_list,
        "attitude": attitude_list,
        "modes": modes_list,
        "messages": messages_list,
        "params": params,
        "vehicle_type": vehicle_type,
    }

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
