import gzip
import os, os.path

results_folder = "urban_locfield"
ground_truth_file = "../sample_dataset/user_groundtruth_locfield.tsv"
output_file = "summary/urban_locfield.txt"
fold_count = 6

def main():
    idToGender = {}
    idToUrbanLevel = {}
    with open(ground_truth_file, "r") as gt_file:
        gt_file.next()
        for line in gt_file:
            try:
                uid, gender, urbanLevel = line.split('\t')
                idToGender[uid] = gender
                idToUrbanLevel[uid] = int(urbanLevel)
            except:
                pass

    male_percents = [0] * 10
    female_percents = [0] * 10
    unknown_percents = [0] * 10

    level_1_percents = [0] * 10
    level_2_percents = [0] * 10
    level_3_percents = [0] * 10
    level_4_percents = [0] * 10
    level_5_percents = [0] * 10
    level_6_percents = [0] * 10

    for i in range(fold_count):
        with gzip.open(os.path.join(results_folder, "fold_" + str(i) + ".results.tsv.gz"), "r") as fold_file:
            fold_file.next()

            county_totals = [0] * 6
            county_ideals = [0] * 6

            gender_totals = {}
            gender_ideals = {}
            
            gender_totals["m"] = 0
            gender_ideals["m"] = 0
            gender_totals["f"] = 0
            gender_ideals["f"] = 0
            gender_totals["n"] = 0
            gender_ideals["n"] = 0

            for line in fold_file:
                try:
                    uid, known_lat, known_lon, pred_lat, pred_lon, distance = line.split('\t')
                    if distance.strip() != "none":
                        gender = idToGender.get(uid, -1)
                        urbanLevel = idToUrbanLevel.get(uid, -1)
                        distance = float(distance)
                        if gender != -1:
                            gender_totals[gender] += 1
                            if distance <= 100:
                                gender_ideals[gender] += 1
                        if urbanLevel != -1:
                            print(str(urbanLevel) + " " + str(distance))
                            county_totals[urbanLevel-1] += 1
                            if distance <= 100:
                                county_ideals[urbanLevel-1] += 1
                except:
                    pass

            print(gender_ideals["m"])
            male_percents[i] = float(gender_ideals["m"]) / max(gender_totals["m"], 1.0) * 100
            female_percents[i] = float(gender_ideals["f"]) / max(gender_totals["f"], 1.0) * 100
            unknown_percents[i] = float(gender_ideals["n"]) / max(gender_totals["n"], 1.0) * 100

            level_1_percents[i] = float(county_ideals[0]) / max(county_totals[0], 1) * 100
            level_2_percents[i] = float(county_ideals[1]) / max(county_totals[1], 1) * 100
            level_3_percents[i] = float(county_ideals[2]) / max(county_totals[2], 1) * 100
            level_4_percents[i] = float(county_ideals[3]) / max(county_totals[3], 1) * 100
            level_5_percents[i] = float(county_ideals[4]) / max(county_totals[4], 1) * 100
            level_6_percents[i] = float(county_ideals[5]) / max(county_totals[5], 1) * 100

    with open(output_file, "w") as fh:
        fh.write("male percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % male_percents[i])
        fh.write("\n")

        fh.write("female percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % female_percents[i])
        fh.write("\n")

        fh.write("unknown gender percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % unknown_percents[i])
        fh.write('\n')

        fh.write("Level 1 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_1_percents[i])
        fh.write('\n')

        fh.write("Level 2 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_2_percents[i])
        fh.write('\n')

        fh.write("Level 3 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_3_percents[i])
        fh.write('\n')

        fh.write("Level 4 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_4_percents[i])
        fh.write('\n')

        fh.write("Level 5 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_5_percents[i])
        fh.write('\n')

        fh.write("Level 6 percents: ")
        for i in range(fold_count):
            fh.write("%f%% " % level_6_percents[i])
        fh.write('\n')

   
if __name__ == "__main__":
    main()
