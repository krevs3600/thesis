import requests
import json

# URL of the Axum server
url = "http://localhost:3000/preprocess_event"

# Sample event data
event = {
    "event_time": "2024-12-09T10:15:30Z"
}

try:
    # Send a POST request
    response = requests.post(url, json=event)
    
    # Check the server's response
    if response.status_code == 200:
        print("Event processed successfully:")
        print(response.json())  # Assuming server responds with JSON
    else:
        print(f"Failed to process event. Status code: {response.status_code}")
        print("Response:", response.text)
except Exception as e:
    print("An error occurred:", e)

