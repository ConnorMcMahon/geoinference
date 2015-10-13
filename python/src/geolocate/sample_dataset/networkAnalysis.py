import networkx as nx

ground_truth_file = "user_groundtruth_locfield.tsv"
bimention_file = "bi_mention_network.elist"
output_file = "graph_summary.txt"
filtered_ground_truth_file = "filtered_user_groundtruth_locfield.tsv"

def main():
    goldStats = {}
    goldStats["m"] = set()
    goldStats["f"] = set()
    goldStats["n"] = set()
    goldStats[1] = set()
    goldStats[2] = set()
    goldStats[3] = set()
    goldStats[4] = set()
    goldStats[5] = set()
    goldStats[6] = set() 
    
    idToGender = {}  
    idToUrbanLevel = {}
    with open(ground_truth_file, "r") as gt_file:
        gt_file.next();
        for line in gt_file:
            try:
                uid, gender, urbanLevel = line.split('\t')
                idToGender[uid] = gender
                goldStats[gender].add(uid)
                if urbanLevel != "\r\n":
                    idToUrbanLevel[uid] = int(urbanLevel)
                    goldStats[int(urbanLevel)].add(uid)
            except:  
                print line

    bimention_graph = nx.Graph()

    userStats = {}
    userStats["m"] = set()
    userStats["f"] = set()
    userStats["n"] = set()
    userStats[1] = set()
    userStats[2] = set()
    userStats[3] = set()
    userStats[4] = set()
    userStats[5] = set()
    userStats[6] = set()

    with open(bimention_file, "r") as fh:
        for line in fh:
            u1, u2 = line.split(" ")
            bimention_graph.add_edge(u1, u2)

    for user in bimention_graph.nodes():
        gender = idToGender.get(user, -1)
        if gender != -1:
            userStats[gender].add(user)

        urbanLevel = idToUrbanLevel.get(user, -1)
        if urbanLevel != -1:
            userStats[urbanLevel].add(user)
    
    reachableNodes = set(bimention_graph.nodes())
    with open(filtered_ground_truth_file, "w") as new_file:
        new_file.write("uid\tgender\turban")
        
        with open(ground_truth_file, "r") as gt_file:
            gt_file.next()
            for line in gt_file:
                uid = line.split('\t')[0]
                if uid in reachableNodes:
                    new_file.write(line)
            
    with open(output_file, "w") as fh:
        fh.write("male #: " + str(len(userStats["m"]))+ "\t" + str(len(goldStats["m"])) + "\n")
        fh.write("female #: " + str(len(userStats["f"])) + "\t" + str(len(goldStats["f"]))  + "\n")
        fh.write("unknown #: " + str(len(userStats["n"]))+ "\t" + str(len(goldStats["n"])) + "\n")

        fh.write("level 1 #: " + str(len(userStats[1]))+ "\t" + str(len(goldStats[1])) + "\n")
        fh.write("level 2 #: " + str(len(userStats[2]))+ "\t" + str(len(goldStats[2])) + "\n")
        fh.write("level 3 #: " + str(len(userStats[3]))+ "\t" + str(len(goldStats[3])) + "\n")
        fh.write("level 4 #: " + str(len(userStats[4]))+ "\t" + str(len(goldStats[4])) + "\n")
        fh.write("level 5 #: " + str(len(userStats[5]))+ "\t" + str(len(goldStats[5])) + "\n")
        fh.write("level 6 #: " + str(len(userStats[6]))+ "\t" + str(len(goldStats[6])) + "\n")


if __name__ == "__main__":
    main()


