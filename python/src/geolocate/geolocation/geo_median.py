__author__ = 'joh12041'

# Code largely taken from: https://gist.github.com/endolith/2837160
#  with some help from https://github.com/ahwolf/meetup_location/blob/master/code/geo_median.py
#  and adapted to support great circle distances over Euclidean.

from geopy.distance import vincenty
from geopy.distance import great_circle
import csv
import numpy
import traceback

LIMIT_MAD = 30  # acceptable km limit to median absolute deviation of points
LIMIT_POINTS = 3  # acceptable minimum number of GPS points for a user
DISTANCE_THRESHOLD = 1  # distance (meters) between iterations that determines end of search
DATA_POINTS_FILE = '../sample_dataset/post_location_info.csv'
OUTPUT_MEDIANS = '../sample_dataset/users.home-locations.geo-median.tsv'
SNAP_TO_USER_POINTS = True
OUTPUT_ALL_USERS = False
OUTPUT_DELIMITER = '\t'

def main():
    compute_medians()

def compute_medians(iterations=1000, already_computed=None):

    numIter = iterations  # numIter depends on how long it take to get a suitable convergence of objFunc
    count = 0
    medians_found = 0

    already_computed_users = {}
    if already_computed:
        for file in already_computed:
            with open(file, 'r') as fin:
                csvreader = csv.reader(fin)
                assert next(csvreader) == ['uid', 'median']
                for line in csvreader:
                    already_computed_users[line[0]] = True

    with open(DATA_POINTS_FILE, 'r') as fin:
        csvreader = csv.reader(fin)
        assert next(csvreader) == ['uid','lat','lon']
        with open(OUTPUT_MEDIANS, 'w') as fout:
            csvwriter = csv.writer(fout, delimiter=OUTPUT_DELIMITER)
            csvwriter.writerow(['uid','lat','lon'])
            line = next(csvreader)
            dataPoints = [(float(line[1]), float(line[2]))]
            current_uid = line[0]
            for line in csvreader:
                if line[0] == current_uid:
                    dataPoints.append((float(line[1]), float(line[2])))
                else:
                    count += 1
                    if count % 2500 == 0:
                        print("Processed {0} users and {1} medians found.".format(count, medians_found))

                    if current_uid not in already_computed_users:
                        medians_found += compute_user_median(dataPoints, numIter, csvwriter, current_uid)

                    # set user and restart array for new current user
                    current_uid = line[0]
                    dataPoints = [(float(line[1]), float(line[2]))]
            # compute final user's median
            medians_found += compute_user_median(dataPoints, numIter, csvwriter, current_uid)
    print("Processed {0} users and {1} medians found.".format(count, medians_found))


def compute_user_median(dataPoints, numIter, csvwriter, current_uid):
    if len(dataPoints) < LIMIT_POINTS:  # Insufficient points for the user - don't record median
        if OUTPUT_ALL_USERS:
            csvwriter.writerow([current_uid, None, None])
            return 0
    else:
        if SNAP_TO_USER_POINTS: # ensure median is one of the user's points
            lowestDev = float("inf")
            for point in dataPoints:
                tmpAbsDev = objfunc(point, dataPoints)
                if tmpAbsDev < lowestDev:
                    lowestDev = tmpAbsDev
                    testMedian = point
        else:
            testMedian = candMedian(dataPoints)  # Calculate centroid more or less as starting point
            if objfunc(testMedian, dataPoints) != 0:  # points aren't all the same

                #iterate to find reasonable estimate of median
                for x in range(0, numIter):
                    denom = denomsum(testMedian, dataPoints)
                    nextLat = 0.0
                    nextLon = 0.0

                    for y in range(0, len(dataPoints)):
                        nextLat += (dataPoints[y][0] * numersum(testMedian, dataPoints[y]))/denom
                        nextLon += (dataPoints[y][1] * numersum(testMedian, dataPoints[y]))/denom

                    prevMedian = testMedian
                    testMedian = (nextLat, nextLon)
                    try:
                        if vincenty(prevMedian, testMedian).meters < DISTANCE_THRESHOLD:  # 1 meter
                            break
                    except:
                        if great_circle(prevMedian, testMedian).meters < DISTANCE_THRESHOLD:  # 1 meter
                            break

                if x == numIter - 1:
                    print('{0}: failed to converge. Last change between iterations was {1} meters.'.format(current_uid, great_circle(prevMedian, testMedian).meters))

        # Check if user points are under the limit median absolute deviation
        if checkMedianAbsoluteDeviation(dataPoints, testMedian) <= LIMIT_MAD:
            csvwriter.writerow([current_uid, round(testMedian[0],6), round(testMedian[1],6)])
            return 1
        else:
            if OUTPUT_ALL_USERS:
                csvwriter.writerow([current_uid, None, None])
                return 0



def candMedian(dataPoints):
    #Calculate the first candidate median as the geometric mean
    tempLat = 0.0
    tempLon = 0.0

    for i in range(0, len(dataPoints)):
        tempLat += dataPoints[i][0]
        tempLon += dataPoints[i][1]

    return (tempLat / len(dataPoints), tempLon / len(dataPoints))

def checkMedianAbsoluteDeviation(dataPoints, median):
    # Calculate Median Absolute Deviation of a set of points
    distances = []
    for i in range(0, len(dataPoints)):
        try:
            distances.append(vincenty(median, dataPoints[i]).kilometers)
        except ValueError:
            # Vincenty doesn't always converge so fall back on great circle distance which is less accurate but always converges
            distances.append(great_circle(median, dataPoints[i]).kilometers)
    return(numpy.median(distances))

def numersum(testMedian, dataPoint):
    # Provides the denominator of the weiszfeld algorithm depending on whether you are adjusting the candidate x or y
    try:
        return 1 / vincenty(testMedian, dataPoint).kilometers
    except ZeroDivisionError:
        traceback.print_exc()
        return 0  # filter points that equal the median out (otherwise no convergence)
    except ValueError:
        # Vincenty doesn't always converge so fall back on great circle distance which is less accurate but always converges
        return 1 / great_circle(testMedian, dataPoint).kilometers

def denomsum(testMedian, dataPoints):
    # Provides the denominator of the weiszfeld algorithm
    temp = 0.0
    for i in range(0, len(dataPoints)):
        try:
            temp += 1 / vincenty(testMedian, dataPoints[i]).kilometers
        except ZeroDivisionError:
            print('zerodivisionerror', dataPoints[i])
            continue  # filter points that equal the median out (otherwise no convergence)
        except ValueError:
            # Vincenty doesn't always converge so fall back on great circle distance which is less accurate but always converges
            temp += 1 / great_circle(testMedian, dataPoints[i]).kilometers
    return temp

def objfunc(testMedian, dataPoints):
    # This function calculates the sum of linear distances from the current candidate median to all points
    # in the data set, as such it is the objective function that we are minimising.
    temp = 0.0
    for i in range(0, len(dataPoints)):
        try:
            temp += vincenty(testMedian, dataPoints[i]).kilometers
        except ValueError:
            # Vincenty doesn't always converge so fall back on great circle distance which is less accurate but always converges
            temp += great_circle(testMedian, dataPoints[i]).kilometers
    return temp


if __name__ == "__main__":
    main()
