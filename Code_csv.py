import csv
import datetime
import pandas as pd
import re

# Đường dẫn các file
class data:
    #constructor
    def __init__(self, path1, path2, path_new, path_column, path_after_process):
        self.path1 = path1
        self.path2 = path2
        self.path_new = path_new
        self.path_column = path_column
        self.path_after_process = path_after_process

    # list để lưu dữ liệu đọc ra rong các file
    data1 = []
    data2 = []

    # list để lưu dữ liệu được lấy ra các file
    new_data = []
    new_head = []

    """Đọc dữ liệu 2 file rồi thêm vào list"""
    # Read data1
    def read_file(self, idx):
        with open(self.path1, "r") as file1:
            csv_reader = csv.reader(file1, delimiter=',')
            for row in csv_reader:
                if len(row) > 0:
                    self.data1.append(row)

    # Read data2
        with open(self.path2, "r") as file2:
            csv_reader = csv.reader(file2, delimiter=',')
            for row in csv_reader:
                if len(row) > 0:
                    self.data2.append(row)

        if idx == 1:
            print(self.data1[:10])
        elif idx == 2:
            print(self.data2[:10])

    """ Gộp dữ liệu 2 file"""
    def join_file(self):
        for i in range(len(self.data1)):
            del self.data1[i][0]
        #     # del death_data[i][2]
        new_data_join = list()
        for idx in zip(self.data1, self.data2):
            new_data_join.append((idx[0] + idx[1]))

        with open(self.path_new, mode="w", newline='') as new_file:
            new_file_writer = csv.writer(new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i in new_data_join:
                new_file_writer.writerow(i)
        print("\nThe file has been compiled successfully !!!")
        new = pd.read_csv("new_data.csv")
        print("Enter the first 10 lines of the new file: \n",new.head(10))


    def select_column(self, idx):
        new = pd.read_csv("new_data.csv")
        das = new.to_dict('series')
        DICT = dict()
        for id in idx:
            ID_name = list(das.keys())[id - 1]
            DICT[ID_name] = das[ID_name]
        print("Select the columns you want to join: ",idx)
        print("The columns you want to join have been saved in: {0}".format(self.path_column))
        pd.DataFrame(DICT).to_csv(self.path_column, index=False)


    def processing_data(self):
        new_process = []
        with open(self.path_column, "r") as file1:
            csv_reader = csv.reader(file1, delimiter=',')
            for row in csv_reader:
                if len(row) > 0:
                    new_process.append(row)

        for i in range(len(new_process)):
            for j in range(len(new_process[i])):
                x = re.findall("[!@#$%^&*()~`?]", new_process[i][j])
                if new_process[i][j] == "" or new_process[i][j] == " " or new_process[i][j].lower() == "null":
                    new_process[i][j] = '0'
                elif new_process[i][j] == "0":
                    new_process[i][j] = 'null'
                if x:
                    new_process[i][j] = "9"

        with open(self.path_after_process, mode="w", newline='') as new_file:
            new_file_writer = csv.writer(new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i in new_process:
                new_file_writer.writerow(i)

        print("The data has been processed!!")
        print("The columns you want to join have been saved in: {0}".format(self.path_after_process))


if __name__ == "__main__":
    path1 = "file1.csv"
    path2 = "file2.csv"
    path_new = r"new_data.csv"
    path_column = r"column_data.csv"
    path_after_process = r"process.csv"
    obj_data = data(path1, path2, path_new, path_column, path_after_process)
    while True:
        print("=================MENU====================")
        print("+, Enter '1' to display 10 lines of file1")
        print("+, Enter '2' to display 10 lines of file2")
        print("+, Enter '3' to display the joined file !!")
        print("+, Enter '0' to stop the program")
        INPUT = int(input("Please enter your choice: "))
        obj_data.read_file(INPUT)
        if INPUT == 3:
            obj_data.join_file()
            INPUT = input("So. Do you want to join each column as you like? yes/no: ").lower().strip()

            if INPUT == 'yes':
                idx = list(map(int, input("Enter the columns you want to join: ").split(" ")))
                obj_data.select_column(idx)
                idx = input("Do you want to clean up the data? yes/no: ").lower().strip()

                if INPUT == 'yes':
                    obj_data.processing_data()

                else:
                    exit()
            else:
                exit()
        elif INPUT == 0:
            exit()