import threading
import requests
import json
import time
import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import streamlit as st
import base64
import pandas as pd
from io import BytesIO
from PIL import Image

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

@app.route('/ask', methods=['POST'])
def ask():
    question = request.json.get('question')
    if not question:
        return jsonify({"error": "No question provided"}), 400

    base_url = os.getenv("DATABRICKS_BASE_URL")
    token = os.getenv("DATABRICKS_TOKEN")
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Start the job run
    run_now_url = f"{base_url}/jobs/run-now"
    payload = {
        "job_id": 1065737057597852,
        "notebook_params": {
            "question": question
        }
    }
    response = requests.post(run_now_url, headers=headers, json=payload)
    if response.status_code != 200:
        return jsonify({"error": "Failed to run notebook", "details": response.text}), response.status_code

    run_id = response.json().get('run_id')

    # Wait for the job to complete
    get_run_url = f"{base_url}/jobs/runs/get"
    while True:
        run_response = requests.get(get_run_url, headers=headers, params={"run_id": run_id})
        run_info = run_response.json()
        if run_info['state']['life_cycle_state'] in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
            break
        time.sleep(3)  # Reduce wait time to 3 seconds

    # Fetch output for each task
    get_output_url = f"{base_url}/jobs/runs/get-output"
    all_outputs = []
    for task in run_info['tasks']:
        task_run_id = task['run_id']
        output_response = requests.get(get_output_url, headers=headers, params={"run_id": task_run_id})
        if output_response.status_code == 200:
            all_outputs.append(output_response.json())
        else:
            all_outputs.append({"error": f"Failed to get output for task {task['task_key']}"})

    return jsonify(all_outputs)

def start_flask():
    app.run(host='0.0.0.0', port=5000)

def start_streamlit():
    st.title('Streamlit for Sales Usecase')
    st.write('This is a Streamlit app for the Sales Usecase.')

    # Streamlit Chatbox
    request_url = "http://localhost:5000/ask"  # This will be used to interact with Flask

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["type"] == "text":
                st.markdown(message["content"])
            elif message["type"] == "image":
                img_data = message["content"].split(",")[1]
                img = Image.open(BytesIO(base64.b64decode(img_data)))
                st.image(img)
            elif message["type"] == "table":
                table_df = pd.read_json(BytesIO(message["content"].encode('utf-8')))
                st.table(table_df)
            elif message["type"] == "json":
                try:
                    json_data = pd.read_json(BytesIO(message["content"].encode('utf-8')))
                    st.json(json_data)
                except ValueError:
                    st.json(message["content"])

    if prompt := st.chat_input("Type your Question..."):
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "type": "text", "content": prompt})
        
        # Get response from the backend
        try:
            response = requests.post(request_url, json={"question": prompt})
            response.raise_for_status()  # Ensure we raise an error for bad responses
            response_json = response.json()
            
            # Check if the response contains 'error'
            if 'error' in response_json:
                st.error(response_json['error'])
                st.session_state.messages.append({"role": "bot", "type": "text", "content": response_json['error']})
            else:
                notebook_response = response_json[0].get("notebook_output", {}).get("result", "")

                # Display bot response in chat message container
                # Check if the response is a base64 string (assume image if it starts with 'data:image')
                if notebook_response.startswith("data:image"):
                    img_data = notebook_response.split(",")[1]
                    img = Image.open(BytesIO(base64.b64decode(img_data)))
                    st.image(img)
                    st.session_state.messages.append({"role": "bot", "type": "image", "content": notebook_response})
                # Check if the response is JSON data (assume table if it starts with '{' or '[')
                elif notebook_response.startswith("{") or notebook_response.startswith("["):
                    try:
                        response_data = pd.read_json(BytesIO(notebook_response.encode('utf-8')))
                        st.table(response_data)
                        st.session_state.messages.append({"role": "bot", "type": "table", "content": notebook_response})
                    except ValueError:
                        # Handle the case where JSON is not in table format
                        st.json(notebook_response)
                        st.session_state.messages.append({"role": "bot", "type": "json", "content": notebook_response})
                else:
                    st.markdown(notebook_response)
                    st.session_state.messages.append({"role": "bot", "type": "text", "content": notebook_response})
        except requests.exceptions.RequestException as e:
            st.error(f"An error occurred: {e}")
            st.session_state.messages.append({"role": "bot", "type": "text", "content": f"An error occurred while processing your request: {e}"})
        except json.JSONDecodeError as e:
            st.error(f"JSON decode error: {e}")
            st.session_state.messages.append({"role": "bot", "type": "text", "content": f"Failed to decode JSON response: {e}"})

if __name__ == '__main__':
    flask_thread = threading.Thread(target=start_flask)
    flask_thread.start()
    start_streamlit()
