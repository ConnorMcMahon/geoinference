__author__ = 'joh12041'

"""
Gender code largely taken from https://github.com/tapilab/twcounty/blob/master/twcounty/Demographics.ipynb.
A major thanks to Aron Culotta for posting his code and doing a fine job with it in the first place!
"""

# Classify users as male or female based on first names based on Census name frequency.
from collections import defaultdict
import re
import requests
import csv
import json
import os

def names2dict(url):
    """ Fetch data from census and parse into dict mapping name to frequency. """
    names = defaultdict(lambda: 0)
    for line in requests.get(url).text.split('\n'):
        parts = line.lower().split()
        if len(parts) >= 2:
            names[parts[0]] = float(parts[1])
    return names

def getCensusNames():
    """ Fetch census name data and remove ambiguous names. """
    print("Gathering male/female name mappings from Census.")
    males = names2dict('http://www2.census.gov/topics/genealogy/1990surnames/dist.male.first')
    females = names2dict('http://www2.census.gov/topics/genealogy/1990surnames/dist.female.first')
    # print(len(set(list(males.keys()) + list(females.keys()))), 'total names')
    eps = 10.  # keep names that are eps times more frequent in one gender than the other.
    tokeep = []
    for name in set(list(males.keys()) + list(females.keys())):
        mscore = males[name]
        fscore = females[name]
        if mscore == 0 or fscore == 0 or mscore / fscore > eps or fscore / mscore > eps:
            tokeep.append(name)
    print(len(tokeep), 'names for gender determination.')
    m = set([n for n in tokeep if males[n] > females[n]])
    f = set([n for n in tokeep if females[n] > males[n]])
    return m, f


def labelGender(tweet, males, females):
    """ Classify a tweet as male (m) female (f) or neutral (n) based on first token in name field. """
    #name = tweet['user']['name'].lower().split()
    name = tweet.lower().split()
    if len(name) == 0:
        name = ['']
    name = re.findall('\w+', name[0])
    if len(name) == 0:
        name = ''
    else:
        name = name[0]
    if name in males:
        return 'm'
        tweet['user']['gender'] = 'm'
    elif name in females:
        return 'f'
        tweet['user']['gender'] = 'f'
    else:
        return 'n'
        tweet['user']['gender'] = 'n'
    return tweet


def get_users_of_interest():
    print("Gathering gold standard users.")
    folder = '/export/scratch2/isaacj/geoinference/python/src/geolocate/sample_dataset/'
    files = os.listdir(folder)
    county_fns = [f for f in files if 'counties' in f and '.tsv' in f and 'names' not in f]
    point_fns = [f for f in files if 'counties' not in f and '.tsv' in f and 'names' not in f]
    uid_idx = 0
    users = {'locfield':{}, 'geomedian':{}}
    for file in point_fns:
        if 'loc-field' in file:
            gold = 'locfield'
        elif 'geo-median' in file:
            gold = 'geomedian'
        print("Processing {0}".format(folder + file))
        with open(folder + file, 'r') as fin:
            csvreader = csv.reader(fin, delimiter='\t')
            assert next(csvreader) == ['uid','lat','lon']
            for line in csvreader:
                uid = line[uid_idx]
                users[gold][uid] = {'gender':'n', 'urban':None}

    print("Mapping gold standard users counties to urban-rural classifications.")
    fips_to_urban = get_urban_rural_codes()
    for file in county_fns:
        urban_counts = {'1':0, '2':0, '3':0, '4':0, '5':0, '6':0}
        if 'loc-field' in file:
            gold = 'locfield'
        elif 'geo-median' in file:
            gold = 'geomedian'
        print("Processing {0}".format(folder + file))
        with open(folder + file, 'r') as fin:
            csvreader = csv.reader(fin, delimiter='\t')
            assert next(csvreader) == ['uid','county']
            for line in csvreader:
                uid = line[uid_idx]
                county = line[1][:5]  # multiple counties for Twin Cities/NYC but they're all the same classification
                urban = fips_to_urban[county]
                urban_counts[urban] += 1
                users[gold][uid]['urban'] = urban
        print('Breakdown of users to urban-rural classes: {0}'.format(urban_counts))

    return users

def get_urban_rural_codes():
    print("Generating county FIPS to urban-rural classification mapping.")
    fn = '/export/scratch2/isaacj/fips_to_urban.csv'
    fips_to_urban = {}
    with open(fn, 'r') as fin:
        csvreader = csv.reader(fin)
        assert next(csvreader) == ['fips','urban']
        for line in csvreader:
            fips = line[0]
            if len(fips) == 4:
                fips = '0' + fips
            urban = line[1]
            fips_to_urban[fips] = urban

    return fips_to_urban

def get_user_names():
    print("Generating Twitter uid to name mapping.")
    fn = '/export/scratch2/isaacj/twitter_names.csv'
    users_to_names = {}
    with open(fn, 'r') as fin:
        csvreader = csv.reader(fin)
        assert next(csvreader) == ['name','uid']
        for line in csvreader:
            uid = line[1]
            name = line[0]
            users_to_names[uid] = name

    return users_to_names


def network_based_groundtruth():
    males, females = getCensusNames()
    users = get_users_of_interest()
    users_to_names = get_user_names()

    for gold in users:
        print("Labeling gender for {0}.".format(gold))
        gender_counts = {'m':0, 'f':0, 'n':0}
        for uid in users[gold]:
            gender = labelGender(users_to_names[uid], males, females)
            users[gold][uid]['gender'] = gender
            gender_counts[gender] += 1
        print("Breakdown of users to gender classes: {0}".format(gender_counts))

    for gold in users:
        filename = '/export/scratch2/isaacj/geoinference/python/src/geolocate/sample_dataset/user_groundtruth_{0}.csv'.format(gold)
        with open(filename, 'w') as fout:
            print("Writing groundtruth to {0}.".format(filename))
            csvwriter = csv.writer(fout)
            csvwriter.writerow(['uid','gender','urban-rural'])
            for user in users[gold]:
                csvwriter.writerow([user, users[gold][user]['gender'], users[gold][user]['urban']])

def main():
    males, females = getCensusNames()
    uids_fn = '/export/scratch2/isaacj/twitter_names.csv'
    print("Labeling gender for {0}.".format(uids_fn))
    with open(uids_fn, 'r') as fin:
        csvreader = csv.reader(fin)
        header = next(csvreader)
        assert header == ['name','uid']
        gender_counts = {'m':0, 'f':0, 'n':0}
        with open(uids_fn.replace('.csv','_genderlabeled.csv'), 'w') as fout:
            csvwriter = csv.writer(fout)
            csvwriter.writerow(['uid','gender'])
            for line in csvreader:
                name = line[0]
                uid = line[1]
                gender = labelGender(name, males, females)
                csvwriter.writerow([uid, gender])
                gender_counts[gender] += 1
        print("Breakdown of users to gender classes: {0}".format(gender_counts))

if __name__ == "__main__":
    network_based_groundtruth()