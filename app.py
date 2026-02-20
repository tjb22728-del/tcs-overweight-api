import os
import json
import traceback
import threading
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
import snowflake.connector

app = Flask(__name__)
CORS(app)

SF_USER      = os.environ.get("SF_USER",      "ZMDNZIEQEO_USER")
SF_PASSWORD  = os.environ.get("SF_PASSWORD",  "")
SF_ACCOUNT   = os.environ.get("SF_ACCOUNT",   "redzone-prod_direct_access_reader")
SF_DATABASE  = os.environ.get("SF_DATABASE",  "ZMDNZIEQEO_DB")
SF_WAREHOUSE = os.environ.get("SF_WAREHOUSE", "PROD_DIRECT_ACCESS_WAREHOUSE")

CACHE_FILE = "/tmp/overweight_cache.json"
REFRESH_INTERVAL_HOURS = 6

# In-memory cache
_cache = {"data": None, "refreshed_at": None, "status": "initializing"}
_cache_lock = threading.Lock()


def get_connection():
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=SF_DATABASE,
        warehouse=SF_WAREHOUSE,
    )


QUERY = """
SELECT
    DATE_TRUNC('week', s."completeTime")::DATE              AS week_start,
    d."productName" || ' (' || d."productSku" || ')'        AS product,
    ROUND(AVG(s."value" - s."thresholdTarget"), 2)          AS avg_overweight,
    ROUND(AVG(s."value"), 2)                                AS avg_value,
    ROUND(AVG(s."thresholdTarget"), 2)                      AS avg_target,
    COUNT(*)                                                AS sample_count
FROM ZMDNZIEQEO_DB."tillamook-country-smoker-org"."v_spcsample" s
JOIN ZMDNZIEQEO_DB."tillamook-country-smoker-org"."v_completeddataitem" d
    ON s."runUUID" = d."runUUID"
    AND s."characteristicUUID" = d."characteristicUUID"
    AND d."completeTime" >= DATEADD(week, -26, CURRENT_DATE)
    AND d."void" = false
    AND d."productName" IS NOT NULL
    AND d."productSku" IS NOT NULL
WHERE
    s."completeTime" >= DATEADD(week, -26, CURRENT_DATE)
    AND s."characteristicName" ILIKE '%Product Weight%'
    AND s."thresholdTarget" IS NOT NULL
    AND s."deleted" = false
GROUP BY 1, 2
ORDER BY 2, 1
"""


def refresh_cache():
    global _cache
    print(f"[{datetime.utcnow().isoformat()}] Starting Snowflake refresh...")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(QUERY)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        data = {}
        for row in rows:
            week_start, product, avg_ow, avg_val, avg_tgt, count = row
            product = product.strip()
            if product not in data:
                data[product] = []
            data[product].append({
                "week_start": week_start.strftime("%Y-%m-%d") if hasattr(week_start, "strftime") else str(week_start),
                "avg_overweight": float(avg_ow) if avg_ow is not None else 0,
                "avg_value": float(avg_val) if avg_val is not None else 0,
                "avg_target": float(avg_tgt) if avg_tgt is not None else 0,
                "count": count,
            })

        refreshed_at = datetime.utcnow().isoformat() + "Z"

        # Save to file as backup
        with open(CACHE_FILE, "w") as f:
            json.dump({"data": data, "refreshed_at": refreshed_at, "product_count": len(data)}, f)

        with _cache_lock:
            _cache = {"data": data, "refreshed_at": refreshed_at, "status": "ok", "product_count": len(data)}

        print(f"[{datetime.utcnow().isoformat()}] Cache refreshed — {len(data)} products loaded.")

    except Exception as e:
        full_trace = traceback.format_exc()
        print(f"CACHE REFRESH ERROR:\n{full_trace}")
        # Try to load from file backup if available
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE) as f:
                cached = json.load(f)
            with _cache_lock:
                _cache = {**cached, "status": "stale", "error": str(e)}
            print("Loaded stale cache from file.")
        else:
            with _cache_lock:
                _cache = {"data": None, "refreshed_at": None, "status": "error", "error": str(e)}


def schedule_refresh():
    """Run refresh immediately, then every REFRESH_INTERVAL_HOURS hours."""
    refresh_cache()
    interval_seconds = REFRESH_INTERVAL_HOURS * 3600
    timer = threading.Timer(interval_seconds, schedule_refresh)
    timer.daemon = True
    timer.start()


# Start background refresh on startup
refresh_thread = threading.Thread(target=schedule_refresh, daemon=True)
refresh_thread.start()


@app.route("/api/overweights", methods=["GET"])
def overweights():
    with _cache_lock:
        cache = dict(_cache)

    if cache["status"] == "initializing":
        return jsonify({
            "status": "initializing",
            "message": "Data is loading from Snowflake, please check back in 60 seconds.",
        }), 202

    if cache["status"] == "error" and cache.get("data") is None:
        return jsonify({
            "status": "error",
            "message": cache.get("error", "Unknown error"),
        }), 500

    return jsonify({
        "status": "ok",
        "refreshed_at": cache.get("refreshed_at"),
        "product_count": cache.get("product_count", 0),
        "cache_status": cache["status"],
        "data": cache["data"],
    })


@app.route("/api/refresh", methods=["POST"])
def force_refresh():
    """Manually trigger a cache refresh."""
    thread = threading.Thread(target=refresh_cache, daemon=True)
    thread.start()
    return jsonify({"status": "ok", "message": "Refresh started, check back in ~60 seconds."})


@app.route("/health", methods=["GET"])
def health():
    with _cache_lock:
        cache_status = _cache.get("status", "unknown")
        refreshed_at = _cache.get("refreshed_at")
    return jsonify({"status": "ok", "cache": cache_status, "refreshed_at": refreshed_at})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
