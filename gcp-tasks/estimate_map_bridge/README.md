This directory holds files for a GCP Function to get a model PM matrix and store it in Firebase, where it can be used by the front end webpage for graphic display purposes. The flow is as follows:

1. A Google Cloud Scheduler Job (`model_matrix_bridge`) fires periodically, which creates a `pubsub` event message on the topic `trigger_model_matrix_bridge`
2. A Google Cloud Function waits for a `pubsub` publishing event on the aforementioned topic, which triggers a python script (`main.py`).
3. The python script calls the Flask API route (`/api/getEstimateMap`), which returns an object of PM estimates and other relevant data. It then stores this JSON object in Firestore to be used by the frontend. 