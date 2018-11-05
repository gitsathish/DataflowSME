python -m exercises.exercise2 --input gs://sme-training/game/small.csv \
  --output_dataset sme \
  --output_tablename exercise2 \
  --runner DataflowRunner \
  --project YOUR_PROJECT \
  --temp_location gs://YOUR_BUCKET/tmp/ \
  --setup_file ./setup.py
