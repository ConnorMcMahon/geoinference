import csv
import argparse

def remove_empty_lines():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_in')
    parser.add_argument('line_idx', default=1, type=int, help='line index to filter on')
    args = parser.parse_args()
    csv_in = args.csv_in
    if '.csv' in csv_in:
        csv_out = csv_in.replace('.csv','_cleaned.csv')
        delim = ','
    elif '.tsv' in csv_in:
        csv_out = csv_in.replace('.tsv','_cleaned.tsv')
        delim = '\t'
    assert csv_in != csv_out
    lines_in = 0
    lines_out = 0
    with open(csv_in, 'r') as fin:
        csvreader = csv.reader(fin, delimiter=delim)
        with open(csv_out, 'w') as fout:
            csvwriter = csv.writer(fout, delimiter=delim)
            for line in csvreader:
                lines_in += 1
                if line[args.line_idx]:
                    lines_out += 1
                    csvwriter.writerow(line)
                if lines_in % 100000 == 0:
                    print("{0} lines in and {1} lines out".format(lines_in, lines_out))
    print("{0} total lines in and {1} lines out (including header)".format(lines_in, lines_out))


if __name__ == "__main__":
    remove_empty_lines()
