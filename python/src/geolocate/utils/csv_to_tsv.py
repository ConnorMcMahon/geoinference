import csv
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_in')
    args = parser.parse_args()
    csv_in = args.csv_in
    tsv_out =  csv_in.replace('.csv','.tsv')
    with open(csv_in, 'r') as fin:
        csvreader = csv.reader(fin)
        with open(tsv_out, 'w') as fout:
            tsvwriter = csv.writer(fout, delimiter='\t')
            #next(csvreader)
            #tsvwriter.writerow(['uid','county'])
            #tsvwriter.writerow(['uid','pt'])
            #tsvwriter.writerow(['uid','gender'])
            for line in csvreader:
                #uid = line[0]
                #county = line[2]
                #pt = line[2]
                #tsvwriter.writerow([uid, pt])
                tsvwriter.writerow(line)
    print("{0} converted to tsv and written out to {1}".format(csv_in, tsv_out))

if __name__ == "__main__":
    main()
