python -m exercises.exercise1 --input gs://sme-training/game/small.csv \
  --output_dataset sme \
  --output_tablename exercise1 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --temp_location gs://YOUR_BUCKET/tmp/ \
  --setup_file ./setup.py
