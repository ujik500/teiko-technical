import csv

data = list(csv.reader(open('cell-count.csv')))
for i in range(len(data)):
    condition = data[i][2]
    treatment = data[i][5]
    sample_type = data[i][8]
    if condition == "melanoma" and treatment == "miraclib" and sample_type == "PBMC":
        response = data[i][6]
        total_count = sum(int(x) for x in data[i][10:15])
        print(",".join(data[i]))