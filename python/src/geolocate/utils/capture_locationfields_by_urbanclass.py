import csv
import argparse
import operator

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    parser.add_argument('urban_class_to_filter')
    parser.add_argument('output_file')
    args = parser.parse_args()

    fips_to_urban_fn = '/export/scratch2/isaacj/fips_to_urban.csv'
    fips_to_urban = {}
    with open(fips_to_urban_fn, 'r') as fin:
        csvreader = csv.reader(fin)
        assert next(csvreader) == ['fips','urban']
        for line in csvreader:
            fips = line[0]
            if len(fips) == 4:
                fips = '0' + fips
            urban = line[1]
            fips_to_urban[fips] = urban

    counts = {}
    with open(args.file, 'r') as fin:
        csvreader = csv.reader(fin)
        header = ['uid','loc_field','county']
        assert next(csvreader) == header
        with open(args.output_file, 'w') as fout:
            csvwriter = csv.writer(fout)
            csvwriter.writerow(header)
            for line in csvreader:
                county = line[2]
                loc_field = line[1].lower()
                if county:
                    urban = fips_to_urban[county.split(';')[0]]
                    if urban == args.urban_class_to_filter:
                        counts[loc_field] = counts.get(loc_field, 0) + 1
                        csvwriter.writerow(line)

    counts_sorted = sorted(counts.items(), key=operator.itemgetter(1))
    for loc_field in counts_sorted:
        print("{0}: {1}".format(loc_field[0], loc_field[1]))

if __name__ == "__main__":
    main()
