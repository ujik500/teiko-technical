import csv

data = list(csv.reader(open('cell-count.csv')))
yes_count = 0
no_count = 0
yes_total = 0
no_total = 0
for i in range(len(data)):
    condition = data[i][2]
    treatment = data[i][5]
    sample_type = data[i][8]
    if condition == "carcinoma" and treatment == "miraclib" and sample_type == "PBMC":
        response = data[i][6]
        total_count = sum(int(x) for x in data[i][10:15])
        if response == 'yes':
            count = int(data[i][10]) 
            percentage = (count / total_count) * 100
            yes_total += percentage
            yes_count += 1
        elif response == 'no':
            count = int(data[i][10])  
            percentage = (count / total_count) * 100
            no_total += percentage
            no_count += 1
        else:
            print("Error: Unexpected response value")
yes_avg = yes_total / yes_count
no_avg = no_total / no_count
print(f"Average b_cell percentage for responders (yes): {yes_avg:.4f}% over {yes_count} samples")
print(f"Average b_cell percentage for non-responders (no): {no_avg:.4f}% over {no_count} samples")