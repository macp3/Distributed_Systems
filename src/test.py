import pandas as pd
def get_token_by_id(ID):
    taxi_list_file = pd.read_csv("taxis.csv")

    return taxi_list_file[taxi_list_file["ID"] == ID].values[0][1]