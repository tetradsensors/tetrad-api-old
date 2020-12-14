```bash
gcloud functions deploy ps_bq_bridge 
--region us-central1
--entry-point main
--ignore-file .gcloudignore
--retry
--runtime python38 
--trigger-event google.pubsub.topic.publish
--trigger-topic projects/tetrad-296715/topics/telemetry
--trigger-resource topic
```

Minimal:
```bash
gcloud functions deploy ps_bq_bridge \
--retry \
--region us-central1 \
--trigger-topic telemetry \
--runtime python38 \
--ignore-file .gcloudignore \
--entry-point ps_bq_bridge
```