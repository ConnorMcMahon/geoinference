##
#  Copyright (c) 2015, Tyler Finethy, David Jurgens
#
#  All rights reserved. See LICENSE file for details
##

"""
Geocoder and Reverse-Geocoder to be used by the Geolocation Project
Allows for multiple dataset inputs
"""

import os, os.path
import csv
import re
import logging
import sys
import gzip
import json

LOGGER = logging.getLogger(os.path.basename(__file__))

class Geocoder(object):

    """
    Geocoder to be used on the Geolocation Inference Project.
    """
    def __init__(self,dataset="geonames"):
        """
        Initializes the "geocoder" dictionary from geonames
        """
        self.abbv_to_state = state_abbv_data()
        self.state_abbv_regex = re.compile(r'(\b' + (r'\b|\b'.join(self.abbv_to_state.keys())) + r'\b)')
        self.lc_name_to_location = {}

        LOGGER.debug("Geocoder loading city-location mapping from %s" % (dataset))


        if dataset == "geonames":
            data = geonames_data()

            line_no = 0
            for line in data[1:]:
                #TODO this city name should be formatted the same as incoming tweets
                city_name = line[0].lower()
                if not city_name:
                    continue

                line_no += 1
                if line_no % 1000000 == 0:
                    LOGGER.debug("currently read %d locations from %s" %
                                 (line_no, dataset))

                lat = float(line[1])
                lon = float(line[2])
                self.lc_name_to_location[city_name] = (lat, lon)


        else:
            raise NotImplementedError(dataset)


        LOGGER.debug("Geocoder loaded %d locations from %s" %
                     (len(self.lc_name_to_location), dataset))


    def geocode_noisy(self, location_name):
        """
        Returns the latitude and longitude (tuple) of a noisy location name
        (e.g., the location field of a social media user's profile).  If your
        input isn't cleaned, you probably want this method instead of geocode().
        """

        usaRegex = re.compile("\\busa\\b")
        usRegex = re.compile("\\bus\\b")
        ukRegex = re.compile("\\buk\\b")
        
        name = location_name.lower()
        name = name.strip()

        # Correct for a few common noisy prefices
        if name.startswith("the city of "):
            name = name[12:] #.substring("the city of ".length())
        if name.startswith("downtown "):
            name = name[9:] #.substring("downtown ".length())

        # Swap out the three common contry abbrevations
        name = re.sub(usaRegex, "united states", name)
        name = re.sub(usRegex, "united states", name)
        name = re.sub(ukRegex, "united kingdom", name)

        # Substitute out state names from the US
        matches = re.search(self.state_abbv_regex, name)
        if not matches is None:
            abbv = matches.group(0)
            expanded = name[:matches.start(0)] + self.abbv_to_state[abbv] + name[matches.end(0):]
            #print "%s:: %s -> %s" % (abbv, name, expanded)
            name = expanded

        # Once we've matched abbreivations, lower case for all further
        # comparisons
        name = name.lower()

        if name == "washington, d.c." or name == "washington dc" or name == "washington, dc":
            return (38.904722, -77.016389)

        # Strip off all the cruft on either side
        name = re.sub(r'^[\W+]+', " ", name)
        name = re.sub(r'[\W+]+$', " ", name)
        name = name.strip()

        # Rename the dict for brevity since we're going to referencing it a lot
        # in the next section
        locs = self.lc_name_to_location

        # Last ditch effort: just try matching the whole name and hope it's
        # a single unambiguous city match
        if name in locs:
            return locs[name]

        try:
            if name.find(':') > 0:
                idx = name.find(':')
                coords = name[idx+1:].replace(' ','').split(',')
                return (float(coords[0]), float(coords[1]))
            else:
                coords = name.replace(' ','').split(',')
                return (float(coords[0]), float(coords[1]))
        except:
            pass

#        print "SEACHING %s..." % (name)

        # Look for some name delimeters in the name to try matching on
        # city/state, etc.
        if name.find(',') >= 0 or name.find('-') >= 0 or name.find('|') >= 0:
            parts = re.split(r'[,\-|]+', name)

            if len(parts) == 2:
                p1 = parts[0].strip()
                p2 = parts[1].strip()
                # print "CASE1: (%s) (%s)" % (p1, p2)
                if p1 + '\t' + p2 in locs:
                    return locs[p1 + '\t' + p2]
                elif p2 + '\t' + p1 in locs:
                    return locs[p2 + '\t' + p1]
                elif p1 in locs:
                    return locs[p1]

                if p1.find("st.") >= 0:
                    p1 = re.sub("st.", "saint", p1)
                    if p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p2 + '\t' + p1 in locs:
                        return locs[p2 + '\t' + p1]
                    elif p1 in locs:
                        return locs[p1]

                elif p1.find("saint") >= 0:
                    p1 = re.sub("saint", "st.", p1)
                    if p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p2 + '\t' + p1 in locs:
                        return locs[p2 + '\t' + p1]
                    elif p1 in locs:
                        return locs[p1]

            elif len(parts) == 3:
                p1 = parts[0].strip()
                p2 = parts[1].strip()
                p3 = parts[2].strip()
                # print "CASE2: (%s) (%s) (%s)" % (p1, p2, p3)
                if p1 + '\t' + p2 + '\t' + p3 in locs:
                    return locs[p1 + '\t' + p2 + '\t' + p3]
                elif p1 + '\t' + p2 in locs:
                    return locs[p1 + '\t' + p2]
                elif p1 + '\t' + p3 in locs:
                    return locs[p1 + '\t' + p3]
                elif p1 in locs:
                    return locs[p1]

                if p1.find("st.") >= 0:
                    p1 = re.sub("st.", "saint", p1)
                    if p1 + '\t' + p2 + '\t' + p3 in locs:
                        return locs[p1 + '\t' + p2 + '\t' + p3]
                    elif p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p1 + '\t' + p3 in locs:
                        return locs[p1 + '\t' + p3]
                    elif p1 in locs:
                        return locs[p1]
                if p1.find("saint") >= 0:
                    p1 = re.sub("saint", "st.", p1)
                    if p1 + '\t' + p2 + '\t' + p3 in locs:
                        return locs[p1 + '\t' + p2 + '\t' + p3]
                    elif p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p1 + '\t' + p3 in locs:
                        return locs[p1 + '\t' + p3]
                    elif p1 in locs:
                        return locs[p1]

            else:
                pass #print "CASE5: %s" % (parts)            

        # Otherwise no delimiters so we're left to guess at where the name
        # breaks
        else:
            parts = re.split(r'[ \t\n\r]+', name)
            if len(parts) == 2:
                p1 = parts[0]
                p2 = parts[1]
                #print "CASE3: (%s) (%s)" % (p1, p2)
                if p1 + '\t' + p2 in locs:
                    return locs[p1 + '\t' + p2]
                elif p2 + '\t' + p1 in locs:
                    return locs[p2 + '\t' + p1]
                elif p1 in locs:
                    return locs[p1]
                
                if p1.find("st.") >= 0:
                    p1 = re.sub("st.", "saint", p1)
                    if p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p2 + '\t' + p1 in locs:
                        return locs[p2 + '\t' + p1]
                    elif p1 in locs:
                        return locs[p1]

                elif p1.find("saint") >= 0:
                    p1 = re.sub("saint", "st.", p1)
                    if p1 + '\t' + p2 in locs:
                        return locs[p1 + '\t' + p2]
                    elif p2 + '\t' + p1 in locs:
                        return locs[p2 + '\t' + p1]
                    elif p1 in locs:
                        return locs[p1]


            elif len(parts) > 2:
                # Guess that the last name is a country/state and try
                # city/<whatever>
                #print "CASE4: %s" % (parts)                
                last = parts[-1]
                city = ' '.join(parts[:-1])
                if city + '\t' + last in locs:
                    return locs[city + '\t' + last]
            else:
                pass #print "CASE6: %s" % (parts)


        #print "FOUND? %s ('%s') -> %s" % (location_name, name, lat_lon)

            

        return None



def geonames_data():
    """
    Returns the file contents of the geolite dataset.
    """
    file_contents = []
    file_name = "resources/geonames_countries.tsv"
    with open(file_name, 'r') as fin:
        csvreader = csv.reader(fin, delimiter="\t")
        for line in csvreader:
            file_contents.append(line)
    return file_contents


def state_abbv_data():
    """
    Returns a dict containing state abbreviations
    """
    file_name = "resources/state_table.csv"
    abbv_to_state = {}
    with open(file_name, 'r') as csv_file:
        line_no = 0
        for line in csv.reader(csv_file, delimiter=',', quotechar='"'):
            line_no += 1
            if line_no == 1:
                continue
            name = line[1].lower()
            abbv = line[2].lower()
            if len(abbv) > 0:
                # print "%s -> %s" % (abbv, name)
                abbv_to_state[abbv] = name
    return abbv_to_state

def extract_user_id(post):
    return post["user"]["id_str"]

def extract_user_location(post):
    return post["user"]["location"]

def main():
    # Generate geocoder from preprocessed CSV
    print("Starting...")
    gc = Geocoder()
    print("Geocoder created with {0} places.".format(len(gc.lc_name_to_location)))
    location_field_data_fn = "../sample_tweets.json.gz"
    output_fn = "../sample_dataset/users.home-locations.geo-median.tsv"

    uidToLocations = {}
    fin = gzip.open(location_field_data_fn)
    for line in fin:
        try:
            line = line.strip().replace(r'\\"', r'\"')
            post = json.loads(line)
            uid = extract_user_id(post)
            if(uidToLocations.get(uid, None) == None):
                uidToLocations[uid] = extract_user_location(post)
        except:
            continue
    print len(uidToLocations.keys())

    i = 0
    with open(output_fn, "w") as fout:
        csvwriter = csv.writer(fout, delimiter='\t')
        for uid, location in uidToLocations.iteritems():
            if location:
                pt = gc.geocode_noisy(location)
            else:
                pt = None
            if pt == None:
                continue
            csvwriter.writerow([uid, pt[0], pt[1]])
            i += 1
    print i


'''
    # Loop through user location field entries and geocode them
    with open(location_field_data_fn, "r") as fin:
        csvreader = csv.reader(fin)
        header = ['uid', 'loc_field']
        assert next(csvreader) == header
        with open(output_fn, "w") as fout:
            csvwriter = csv.writer(fout, delimiter='\t')
            csvwriter.writerow(header + ['pt'])
            count_geolocated = 0
            count_tried = 0
            for line in csvreader:
                uid = line[0]
                self_reported_location = line[1]
                if self_reported_location:
                    pt = gc.geocode_noisy(self_reported_location)
                else:
                    pt = None
                if pt = None:
                    continue
                count_tried += 1
                csvwriter.writerow([uid, pt[0], pt[1]])
                if pt:
                    count_geolocated += 1
                if count_tried % 10000 == 0:
                    print("{0} located out of {1} tried.".format(count_geolocated, count_tried))
'''
if __name__ == "__main__":
    main()



