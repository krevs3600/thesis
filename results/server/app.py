from flask import Flask, request, jsonify, render_template
from pathlib import Path
import sqlite3
import os
import subprocess

app = Flask(__name__)

@app.route("/")
def home():
    return "<p>Hello there</p>"


@app.route("/execution_time", methods=['POST'])
def insert_execution_time():
    data = request.get_json(force=True)
    required_fields = ["benchmark", "backend", "test", "query_id", "execution_time", "run_id"]
    # validating the input
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        conn = sqlite3.connect("results.db")
        cursor = conn.cursor()
        query = """
        REPLACE INTO execution_times (benchmark, backend, test, query_id, run_id, execution_time)
        VALUES (?, ?, ?, ?, ?, ?);
        """
        cursor.execute(query, (
            data['benchmark'],
            data['backend'],
            data['test'],
            data['query_id'],
            data["run_id"],
            data['execution_time']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Execution time added"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/execution_times', methods=['GET'])
def get_execution_times():
    try:
        # Connect to the database
        conn = sqlite3.connect("results.db")
        cursor = conn.cursor()
        
        # Query to fetch all records
        query = "SELECT * FROM execution_times;"
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Map results to a list of dictionaries
        results = []
        for row in rows:
            results.append({
                "benchmark": row[0],
                "backend": row[1],
                "test": row[2],
                "query_id": row[3],
                "run_id" : row[4],
                "execution_time": row[5]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/reset', methods=['GET'])
def reset_db():
    try:
        # Connect to the database
        conn = sqlite3.connect("results.db")
        cursor = conn.cursor()
        
        # Query to fetch all records
        query = "DELETE FROM execution_times;"
        cursor.execute(query)
    
        cursor.close()
        conn.close()
        
        return jsonify("OK"), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

 

if __name__ == '__main__':
    app.run(debug=True)