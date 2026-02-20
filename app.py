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
WHERE
    s."completeTime" >= DATEADD(week, -26, CURRENT_DATE)
    AND s."characteristicName" ILIKE '%Product Weight%'
    AND s."thresholdTarget" IS NOT NULL
    AND s."deleted" = false
    AND d."void" = false
    AND d."productName" IS NOT NULL
    AND d."productSku" IS NOT NULL
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
            "product_count": len(data),
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


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
