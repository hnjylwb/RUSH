import asyncio
from flask import Flask, request, jsonify
from src.rush_core import RUSHCore

app = Flask(__name__)
rush = RUSHCore()

# Start background schedulers when Flask app starts
@app.before_first_request
def startup():
    asyncio.create_task(rush.start_background_schedulers())

@app.route('/submit', methods=['POST'])
def submit_query():
    """Submit SQL query for execution"""
    try:
        data = request.get_json()
        if not data or 'sql' not in data:
            return jsonify({"error": "SQL query is required"}), 400
        
        # Run async function in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        query_id = loop.run_until_complete(rush.submit_query(data['sql']))
        loop.close()
        
        return jsonify({
            "query_id": query_id,
            "status": "submitted",
            "message": "Query submitted for execution"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/status/<query_id>')
def get_query_status(query_id):
    """Get status of a specific query"""
    # For demo purposes, return queue status
    return jsonify({
        "query_id": query_id,
        "queue_status": rush.get_queue_status()
    })

@app.route('/')
def root():
    return jsonify({"message": "RUSH - Heterogeneous Cloud Service Scheduler", "version": "1.0.0"})

@app.route('/queues')
def get_queue_status():
    """Get current queue status"""
    return jsonify(rush.get_queue_status())

@app.route('/services')
def get_services():
    """Get available service status"""
    services_info = {}
    for service_type, service_list in rush.services.items():
        services_info[service_type.value] = [
            {
                "name": service.name,
                "available": service.is_available(),
                "config": service.config
            }
            for service in service_list
        ]
    return jsonify(services_info)

if __name__ == "__main__":
    print("Starting RUSH scheduling system...")
    app.run(host="0.0.0.0", port=8000, debug=True)
