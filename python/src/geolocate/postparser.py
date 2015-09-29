import json
import csv
import gzip

def main():
    tweets_file = gzip.open("sample_tweets.json.gz",'r')
    output = open("sample_dataset/post_location_info.tsv", "w")
    tsvwriter = csv.writer(output, delimiter="\t")
    tsvwriter.writerow(['uid', 'lat', 'lon'])
    for line in tweets_file:
        try:
            line = line.strip().replace(r'\\"', r'\"')
            post = json.loads(line)
            uid = post['user']['id_str']
            lat = post['coordinates']['coordinates'][0]
            lon = post['coordinates']['coordinates'][1]
            tsvwriter.writerow([uid,lat,lon])
        except:
            continue



if __name__=="__main__":
    main()

