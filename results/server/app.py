from flask import Flask, request, jsonify, render_template
from pathlib import Path
import sqlite3
import os
import subprocess

THESIS_ROOT = Path(os.environ.get("THESIS_ROOT"))
TPC_ENV = THESIS_ROOT / "tpc_h" / "env.sh"
DUCKDB = THESIS_ROOT / "tpc_h" / "duckdb" / "app.py"
RISINGWAVE_PATH = THESIS_ROOT / "tpc_h" / "risingwave"
FLINK = THESIS_ROOT / "tpc_h" / "flink" / "target" / "flink-1.0-SNAPSHOT.jar"
RENOIR = THESIS_ROOT / "tpc_h" / "renoir" / "target" / "release" / "renoir-tpc"
POLARS = THESIS_ROOT / "tpc_h" / "polars" / "target" / "release" / "polars.tpc"

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
        query = "DELETE * FROM execution_times;"
        cursor.execute(query)
    
        cursor.close()
        conn.close()
        
        return jsonify("OK"), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/tpc_h', methods=['GET'])
def tpc_h():
    return render_template('tpc.html') 
    

# Backend command configuration
BACKEND_COMMANDS = {
    "duckdb": {
        "start": ":",
        "execute": lambda query_idx, iteration, test_name: f"python {DUCKDB} {query_idx} {iteration} {test_name}",
        "stop": ":"
    },
    "risingwave": {
        "start": lambda: f"docker compose -f {str(RISINGWAVE_PATH / 'docker-compose.yaml')} up -d",
        "execute": lambda query_idx, iteration, test_name: f"python {str(RISINGWAVE_PATH / 'app.py')} {query_idx} {iteration} {test_name}",
        "stop": lambda: f"docker compose -f {str(RISINGWAVE_PATH / 'docker-compose.yaml')} down"
    },
    "flink": {
        "start": "start-cluster.sh",
        "execute": lambda query_idx, iteration, test_name: f"flink run {FLINK} {query_idx} {iteration} {test_name}",
        "stop": "stop-cluster.sh"
    },
    "renoir": {
        "start": ":",
        "execute": lambda query_idx, iteration, test_name: f"./{RENOIR} {query_idx} {iteration} {test_name}",
        "stop": ":"
    },
    "polars": {
        "start": ":",
        "execute": lambda query_idx, iteration, test_name: f"./{POLARS} {query_idx} {iteration} {test_name}",
        "stop": ":"
    }
}

# Helper function to execute shell commands
def run_command(command):
    return subprocess.run(
        command,
        shell=True,
        text=True,
        capture_output=True,
        executable="/bin/bash"
    )

@app.route('/start_tpc_test', methods=['POST'])
def start_tpc_test():
    # Extract and validate form data
    backend = request.form.get('backend', 'all')
    query_idx = request.form.get('query', '0')
    iteration = request.form.get('num_runs', '1')
    test_name = request.form.get('test_name', 'tpch')

    # Ensure valid inputs
    if backend not in BACKEND_COMMANDS and backend != "all":
        return jsonify({"status": "error", "message": f"Invalid backend: {backend}"}), 400

    # Define the backends to use
    backends = [backend] if backend != "all" else list(BACKEND_COMMANDS.keys())

    # Source environment
    source_env = f"source {TPC_ENV}"

    results = []

    # Execute commands for each backend
    for backend in backends:
        commands = BACKEND_COMMANDS[backend]

        try:
            # Start backend
            start_command = f"{source_env} && {commands['start']() if callable(commands['start']) else commands['start']}"
            start_result = run_command(start_command)

            # Execute query
            execute_command = f"{source_env} && {commands['execute'](query_idx, iteration, test_name)}"
            execute_result = run_command(execute_command)

            # Stop backend
            stop_command = f"{source_env} && {commands['stop']() if callable(commands['stop']) else commands['stop']}"
            stop_result = run_command(stop_command)

            # Collect results
            results.append({
                "backend": backend,
                "start": start_result.stdout.strip(),
                "execute": execute_result.stdout.strip(),
                "stop": stop_result.stdout.strip()
            })

        except Exception as e:
            results.append({
                "backend": backend,
                "error": str(e)
            })

    return jsonify({
        "status": "success",
        "results": results
    })

if __name__ == '__main__':
    app.run(debug=True)