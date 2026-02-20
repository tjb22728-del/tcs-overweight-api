import os
import traceback
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
SF_SCHEMA    = os.environ.get("SF_SCHEMA",    "tillamook-country-smoker-org")
SF_WAREHOUSE = os.environ.get("SF_WAREHOUSE", "PROD_DIRECT_ACCESS_WAREHOUSE")


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
    DATE_TRUNC('week', "completeTime")::DATE        AS week_start,
    "characteristicName"                             AS product,
    ROUND(AVG("value" - "thresholdTarget"), 2)       AS avg_overweight,
    ROUND(AVG("value"), 2)                           AS avg_value,
    ROUND(AVG("thresholdTarget"), 2)                 AS avg_target,
    COUNT(*)                                         AS sample_count
FROM ZMDNZIEQEO_DB."tillamook-country-smoker-org"."v_spcsample"
WHERE
    "completeTime" >= DATEADD(week, -26, CURRENT_DATE)
    AND "deleted" = false
    AND "value" IS NOT NULL
    AND "thresholdTarget" IS NOT NULL
    AND "thresholdTarget" > 0
    AND "characteristicName" ILIKE '%weight%'
GROUP BY 1, 2
ORDER BY 2, 1
"""


@app.route("/api/overweights", methods=["GET"])
def overweights():
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

        return jsonify({
            "status": "ok",
            "refreshed_at": datetime.utcnow().isoformat() + "Z",
            "data": data,
        })

    except Exception as e:
        full_trace = traceback.format_exc()
        print("FULL ERROR:\n" + full_trace)
        return jsonify({
            "status": "error",
            "message": str(e),
            "detail": full_trace,
        }), 500


@app.route("/api/tables", methods=["GET"])
def list_tables():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute('SHOW VIEWS IN SCHEMA ZMDNZIEQEO_DB."tillamook-country-smoker-org"')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"views": [row[1] for row in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
