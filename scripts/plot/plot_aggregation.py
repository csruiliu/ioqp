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
    
    plt.rcParams["figure.figsize"] = (6,4)
    plt.rcParams.update({'font.size': 12})
    plt.xlabel('Processing Time (s)')
    plt.xticks([0, 50, 100, 150, 200, 250, 300], ['0', '5', '10', '15', '20', '25', '30'])

    # plt.ylabel('Average Price')
    # plt.plot(np.arange(0, len(avg_price_list)), avg_price_list)
    # plt.tight_layout()
    # plt.savefig(fname='q1_100_avg_price.png', dpi='figure', format='png')
    
    # plt.ylabel('Average Quantity')
    # plt.plot(np.arange(0, len(avg_qty_list)), avg_qty_list)
    # plt.tight_layout()
    # plt.savefig(fname='q1_100_avg_quantity.png', dpi='figure', format='png')
    
    # plt.ylabel('Average Discount')
    # plt.plot(np.arange(0, len(avg_disc_list)), avg_disc_list)
    # plt.tight_layout()
    # plt.savefig(fname='q1_100_avg_discount.png', dpi='figure', format='png')
    
    # plt.ylabel('Progress on Average Price')
    # plt.plot(np.arange(0, len(avg_price_list)), normalize_accuracy(avg_price_list,38235.1))  
    # plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1], ['0%', '20%', '40%', '60%', '80%', '100%'])
    # plt.tight_layout()
    # plt.savefig(fname='q1_100_avg_price_progress.png', dpi='figure', format='png')

    # plt.ylabel('Progress on Average Quantity')
    # plt.plot(np.arange(0, len(avg_qty_list)), normalize_accuracy(avg_qty_list, 25.5))
    # plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1], ['0%', '20%', '40%', '60%', '80%', '100%'])
    # plt.tight_layout()
    # plt.savefig(fname='q1_100_avg_quantity_progress.png', dpi='figure', format='png')

    plt.ylabel('Progress on Average Discount')
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1], ['0%', '20%', '40%', '60%', '80%', '100%'])
    plt.plot(np.arange(0, len(avg_disc_list)), normalize_accuracy(avg_disc_list, 0.05))
    plt.tight_layout()
    plt.savefig(fname='q1_100_avg_discount_progress.png', dpi='figure', format='png')

if __name__ == "__main__":
    main()