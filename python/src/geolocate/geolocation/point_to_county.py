__author__ = 'joh12041'

import json
from shapely.wkt import loads
from shapely.geometry import shape
import csv

POINTS_FN = '../sample_dataset/users.home-locations.geo-median.tsv'
OUTPUT_FN = '../sample_dataset/users.home-locations.geo-median.counties.tsv'
EXPECTED_HEADER = ['uid', 'lat', 'lon']
OUTPUT_HEADER = ['uid', 'county']

def main():

    with open("geometries/USCounties_bare.geojson", 'r') as fin:
        counties = json.load(fin)

    for county in counties['features']:
        county['shape'] = shape(county['geometry'])

    with open(POINTS_FN, 'r') as fin:
        csvreader = csv.reader(fin)
        total_points = 0
        points_in_US = 0
        count_lines = 0
        assert next(csvreader) == EXPECTED_HEADER
        uid_idx = EXPECTED_HEADER.index('uid')
        lat_idx = EXPECTED_HEADER.index('lat')
        lon_idx = EXPECTED_HEADER.index('lon')
        US_NORTH_BOUNDARY = 49.6
        US_SOUTH_BOUNDARY = 24.9
        US_EAST_BOUNDARY = -66.9
        US_WEST_BOUNDARY = -125.1

        with open(OUTPUT_FN, 'w') as fout:
            csvwriter = csv.writer(fout)
            csvwriter.writerow(OUTPUT_HEADER)
            for line in csvreader:
                count_lines += 1
                uid = line[uid_idx]
                lat = float(line[lat_idx])
                lon = float(line[lon_idx])
                county = None
                try:
                    if lat and (lat < US_NORTH_BOUNDARY and lat > US_SOUTH_BOUNDARY) and (
                                    lon > US_WEST_BOUNDARY and lon < US_EAST_BOUNDARY):  # quick check point could be in US
                        pt = loads('POINT ({0} {1})'.format(lon, lat))
                        total_points += 1
                        for county_geom in counties['features']:
                            if county_geom['shape'].contains(pt):
                                county = county_geom['properties']['FIPS']
                                points_in_US += 1
                                break
                except Exception as e:
                    print(e)
                    print(line)
                finally:
                    csvwriter.writerow([uid, county])
                    if total_points % 10000 == 0:
                        print ("{0} of {1} points in US and {2} lines in.".format(points_in_US, total_points, count_lines))
    print("{0} of {1} in the US out of {2} total lines.".format(points_in_US, total_points, count_lines))

if __name__ == "__main__":
    main()