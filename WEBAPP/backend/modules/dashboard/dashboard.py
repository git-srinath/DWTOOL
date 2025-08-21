from flask import Blueprint, request, jsonify
from database.dbconnect import create_oracle_connection

# Create blueprint
dashboard_bp = Blueprint('dashboard', __name__)


@dashboard_bp.route("/mappers_and_jobs_count", methods=["GET"])
def dashboard():
    connection = create_oracle_connection()
    cursor = connection.cursor()
    query=""" 
    SELECT 
    (SELECT COUNT(*) FROM DWJOBSCH WHERE CURFLG = 'Y') AS TOTAL_JOBS,
    (SELECT COUNT(*) FROM DWJOBSCH WHERE CURFLG = 'Y' AND STFLG = 'A') AS ACTIVE_JOBS,
    (SELECT COUNT(*) FROM DWJOBSCH WHERE CURFLG = 'Y' AND STFLG = 'N') AS NOT_ACTIVE_JOBS,
    
    (SELECT COUNT(*) FROM DWMAPR WHERE CURFLG = 'Y') AS TOTAL_MAPPINGS,
    (SELECT COUNT(*) FROM DWMAPR WHERE CURFLG = 'Y' AND STFLG = 'A') AS ACTIVE_MAPPINGS,
    (SELECT COUNT(*) FROM DWMAPR WHERE CURFLG = 'Y' AND STFLG = 'N') AS NOT_ACTIVE_MAPPINGS,
    (SELECT COUNT(*) FROM DWMAPR WHERE CURFLG = 'Y' AND STFLG = 'A' AND LGVRFYFLG = 'Y') AS LOGIC_VERIFIED_MAPPINGS,
    (SELECT COUNT(*) FROM DWMAPR WHERE CURFLG = 'Y' AND STFLG = 'A' AND LGVRFYFLG = 'N') AS LOGIC_NOT_VERIFIED_MAPPINGS
    FROM dual
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500  
    


@dashboard_bp.route("/todays-jobs-status", methods=["GET"])
def todays_jobs_status():
    connection = create_oracle_connection()
    cursor = connection.cursor()
    query=""" 
        SELECT STATUS, 
        COUNT(CASE WHEN STATUS = 'PC' THEN 1 END ) AS COMPLETED,
        COUNT(CASE WHEN STATUS = 'IP' THEN 1 END ) AS INPROGRESS,
        COUNT(CASE WHEN STATUS = 'FL' THEN 1 END ) AS FAILED
        FROM DWPRCLOG
        WHERE TRUNC(RECUPDT) = TRUNC(SYSDATE)
        GROUP BY STATUS
        """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return jsonify(rows)    
    except Exception as e:
        return jsonify({"error": str(e)}), 500  



