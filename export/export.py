import logging
from tb_rest_client.rest_client_ce import *
from tb_rest_client.rest import ApiException
import pandas as pd
from getpass import getpass
from datetime import datetime

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# ThingsBoard REST API URL
url = "http://10.100.40.2:8080"

def getDeviceIDfromUser(rest_client):
    devices = {} # dictonary of devices and their ids

    page=0

    while True:
        res = rest_client.get_tenant_device_infos(page_size=10, page=page)

        page += 1

        for device in res.data:
            devices[device.name] = device.id.id
        
        if not res.has_next:
            break

    print("\nAvailable devices:")

    for i, name in enumerate(devices):
        print(f"{i} - {name}")

    number = int(input("\nDevice number to extract data: "))

    id = list(devices.values())[number]

    return id

def getTimestamp(title, time_str):
    print(f"\n{title}")
    
    while True:
        date_str = input("\nEnter the date (format DD/MM/YYYY): ")
        try:
            date = datetime.strptime(date_str, "%d/%m/%Y")
            break
        except ValueError:
            print("Invalid date. Please enter in the format DD/MM/YYYY.")
    
    time = datetime.strptime(time_str, "%H:%M:%S").time()
    
    date_time = datetime.combine(date, time)

    print(f"{title} = {date_time}")

    return int(date_time.timestamp()) * 1000

# Função para dividir o intervalo em partes menores
def get_subintervals(start_ts, end_ts, max_interval):
    intervals = []
    current_start = start_ts
    while current_start < end_ts:
        current_end = int(min(current_start + max_interval, end_ts))
        intervals.append((current_start, current_end))
        current_start = current_end + 1
    return intervals

def main():
    username = ""

    if not username:
        username = input("Username (email): ")
    else:
        print("Username (email):", username)

    password = getpass()

    # Creating the REST client object with context manager to get auto token refresh
    with RestClientCE(base_url=url) as rest_client:
        try:
            # Auth with credentials
            rest_client.login(username=username, password=password)

            # get id to data export
            id = getDeviceIDfromUser(rest_client)

            # EntityId + EntityType
            entityId = EntityId(id, "DEVICE")

            start_ts = getTimestamp("Beginning time", "00:00:00")
            end_ts = getTimestamp("End time", "23:59:59")
            limit = 100000000

            keys = rest_client.get_timeseries_keys_v1(entityId)
            keys = ','.join(keys)

            print(f"\nGetting timeseries keys: {keys}\n")

            max_interval = 1e8

            # Obter os subintervalos
            subintervals = get_subintervals(start_ts, end_ts, max_interval)

            # Inicializar uma lista para armazenar DataFrames
            df_list = []

            # Fazer requests e organizar os dados em um DataFrame
            current = 0
            for (sub_start, sub_end) in subintervals:
                progress = 100*current/len(subintervals)
                current += 1
                print("{:.2f}".format(progress), "%")

                deviceData = rest_client.get_timeseries(entityId, keys, sub_start, sub_end, limit=limit)
                
                df = pd.DataFrame()
                for key, values in deviceData.items():
                    temp_df = pd.DataFrame(values)
                    temp_df = temp_df.rename(columns={'value': key})
                    if df.empty:
                        df = temp_df
                    else:
                        df = pd.merge(df, temp_df, on='ts')
                
                df_list.append(df)


            print("100.00 %\n")

            # Concatenar todos os DataFrames da lista
            combined_df = pd.concat(df_list, ignore_index=True)

            # Remover duplicatas baseadas no timestamp
            df = df.drop_duplicates(subset='ts')

            # Ordena por valor de timestamp
            combined_df = combined_df.sort_values(by='ts').reset_index(drop=True)

            # Salvar o DataFrame final em um arquivo CSV
            combined_df.to_csv('output.csv', index=True)
            
            logging.info("Saved output.csv with timeseries data")

        except ApiException as e:
            logging.exception(e)

if __name__ == '__main__':
    main()
