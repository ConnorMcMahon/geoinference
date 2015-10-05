Commands to run
Note: assumes sample_tweets.json.gz

1. cd geoinference/python/src/geolocate

2. psql -d twitterstream_zh_us
   \COPY (SELECT tweet FROM tweets_as_json_2015_08_19) TO sample_tweets.json
    \d

3. gzip sample_tweets.json

4. setenv LD_PRELOAD /usr/lib/x86_64-linux-gnu/libgfortran.so.3

Sets an environment variable so flagon doesn't yell at you when you try to run the scripts.

5. python postparser.py
Processes the nodes to the form uid, lat, lon in a .tsv file for the next step

6.  cd geolocation
    python geo_median.py
    cd ..

    Builds a record of the "ground truth" location for each user in the dataset. If the user has 3 posts in a 30 km radius, then use the median of those points.

7. gzip sample_dataset/users.home-locations.geo-median.tsv

gzips file for previous step for training step

8. python app.py create_folds sample_dataset 10 cross_folds [TEST_TYPE]

TEST_TYPE = county for urban vs rural, or gender for male/female/unknown
           
9. python app.py cross_validate spatial_label_propagation settings.json sample_dataset cross_folds output


