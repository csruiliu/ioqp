import matplotlib.pyplot as plt
import numpy as np


def normalize_accuracy(input_list:list, groundtruth:float) -> list:
    max_item = max(input_list)
    min_item = max(input_list)
    
    offset = max([abs(max_item - groundtruth), abs(groundtruth - min_item)])

    accuracy_list = map(lambda x: 0 if (1-abs((x - groundtruth)/offset)) < 0 else 1-abs((x - groundtruth)/offset), input_list)
    
    return list(accuracy_list)


def main():
    avg_disc_list = list()
    avg_price_list = list()
    avg_qty_list = list()

    with open('q1_avg_aggregation_100') as f:
        lines = f.readlines()
    
    for line in lines:
        if line.startswith('Avg Aggregation on avg_disc'):
            line = line.replace('Avg Aggregation on avg_disc: ', '')
            avg_disc_list.append(float(line.split()[0]))
        elif line.startswith('Avg Aggregation on avg_qty'):
            line = line.replace('Avg Aggregation on avg_qty: ', '')
            avg_qty_list.append(float(line.split()[0]))
        elif line.startswith('Avg Aggregation on avg_price'):
            line = line.replace('Avg Aggregation on avg_price: ', '')
            avg_price_list.append(float(line.split()[0]))
        else:
            pass

    plt.plot(np.arange(0, len(avg_price_list)), avg_price_list)
    plt.plot(np.arange(0, len(avg_qty_list)), avg_qty_list)
    plt.plot(np.arange(0, len(avg_disc_list)), avg_disc_list)

    plt.plot(np.arange(0, len(avg_price_list)), normalize_accuracy(avg_price_list,38235.1))
    plt.plot(np.arange(0, len(avg_qty_list)), normalize_accuracy(avg_qty_list, 25.5))
    plt.plot(np.arange(0, len(avg_disc_list)), normalize_accuracy(avg_disc_list, 0.05))
    

if __name__ == "__main__":
    main()