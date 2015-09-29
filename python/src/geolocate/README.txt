commands to run

setenv LD_PRELOAD /usr/lib/x86_64-linux-gnu/libgfortran.so.3
python app.py build_dataset sample_dataset sample_tweets.json.gz id_str id_str
python app.py train spatial_label_propagation settings.json sample_dataset sample_model
