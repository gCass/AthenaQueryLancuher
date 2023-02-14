import configparser
import boto3
import json
import time
import asyncio

DEBUG_FLAG = True

def printIfDebug(value):
    if DEBUG_FLAG:
        print(value)

def prepareQueriesToLaunch(config):

    # Parte uno: ricava la lista dei file contenenti le query
    file_with_list_of_query_to_launch = config['DEFAULT']['file_with_list_of_query_to_launch']

    with open(file_with_list_of_query_to_launch, "r") as f:
        files_list = f.read().split('\n')

    conf_date = config['DEFAULT']['launch_date']
    launch_date = f"cast('{conf_date}' as date)"

    queries_to_launch = {}

    for query_file in files_list:

        # TODO: aggiungere check sull'esistenza del percorso. Se esiste un errore stoppare per quella quella e passa alla successiva
        with open(query_file, "r") as f:
            query = f.read()

        query_format_1 = query.replace("now()", launch_date)

        if "he_" in query_file:
            query_format_1 = query_format_1.replace("l3_rep_health.he_polizza", "l3_rep_health_temp.he_polizza_temp")
        
        table_name_tmp = query_file.split("\\")[-1]
        table_name = table_name_tmp.split(".sql")[0]
        queries_to_launch[table_name] = query_format_1

    return queries_to_launch

# Dobbiamo tenerci tempistiche anche di 2 ore, quindi maxexecution deve essere 
# pari 30 secondi * x volte = 2 ore = 180 minuti = 10800 secondi => x=360
async def has_query_succeeded(client, execution_id, max_execution = 360, sleep_time = 30):
    state = "RUNNING"    

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
        max_execution -= 1
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True, response

        # TODO: Le nostre query hanno un ordine di esecuzione di mezzora, oppure di ore,  
        await asyncio.sleep(sleep_time)

    return False, response

def get_query_results(client, execution_id):
    response = client.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet']['Rows']
    return results

async def execute_query(client, query, sleep_time=30):
    # printIfDebug(query)
    try:
        response = client.start_query_execution(QueryString=query)
    except Exception:
        print(query)
        raise Exception(f"The query\n\n {query} \n\n raised an exception")

    query_exec_id = response["QueryExecutionId"]
    query_status, status_reponse = await has_query_succeeded(client, execution_id=query_exec_id, sleep_time=sleep_time)
    return query_exec_id, query_status, status_reponse

async def get_existing_table_list(client, schema="l3_rep_research"):

    existing_tables_query = f"SHOW TABLES IN  {schema}"
    query_exec_id, query_status, status_reponse = await execute_query(client, existing_tables_query, sleep_time=30)
    
    if query_status:
        results = get_query_results(client, query_exec_id)
    else:
        raise Exception(f"Was not possible to check available tables in the schema {schema}. \nStatus\n {status_reponse}")

    l = []
    for data in results:
        for data2 in data['Data']:
            for k in data2:
                v = data2[k]
                l.append(v)
    return l

    
    # return existing_table_list

def check_if_table_exists(table, table_list):
    return table.lower() in table_list or table.upper() in table_list or table in table_list 

async def drop_the_table(client, table):
    drop_query = f"drop table l3_rep_research.{table}  --dropstore"
    query_exec_id, query_status, status_reponse = await execute_query(client, drop_query)
    if query_status:
        result_i = "success"
    else:
        result_i = f"failure.\n {status_reponse['QueryExecution']['Status']['StateChangeReason']}"
    printIfDebug(f"Drop table {table} result: {result_i}")

async def query_iteration(client, query, table_name, table_list, time_to_wait_after_droptable_in_ms=60):
    # query = queries_to_launch[table_name]
    # table_name = k
    printIfDebug(table_name)
    if check_if_table_exists(table_name,table_list):
        await drop_the_table(client, table_name)
        printIfDebug(f"Wait {time_to_wait_after_droptable_in_ms} ms before recreating the table {table_name}")
        await asyncio.sleep(time_to_wait_after_droptable_in_ms)
        printIfDebug(f"Waited {time_to_wait_after_droptable_in_ms} ms before recreating the table {table_name}")

    query_exec_id, query_status, status_reponse = await execute_query(client, query)

    printIfDebug(f"Query id {query_exec_id}")

    if query_status:
        result_i = "success"
    else:
        result_i = f"failure.\n {status_reponse['QueryExecution']['Status']['StateChangeReason']}"
    
    printIfDebug(f"Result of {table_name}: {result_i}")

    
    p = table_name
    json_formatted_str = json.dumps(status_reponse, indent=2, default=str)
    with open(f"results/{p}.json", "w") as f:
        f.write(json_formatted_str)
    
    printIfDebug("========\n\n\n")
    # i += 1
    return result_i


async def execute_all_queries(clientGen, queries_to_launch, debugPrint = False, time_to_wait_after_droptable_in_ms = 60):
    results = {}
    # i = 0
    client = clientGen.getNewClient()
    table_list = await get_existing_table_list(client)
    print(f"Existing tables\n\n{table_list}")
    tasks = []
    for table_name in queries_to_launch:
        query = queries_to_launch[table_name]
        client = clientGen.getNewClient()
        tasks.append(
            query_iteration(
                client, query, table_name, table_list,
                time_to_wait_after_droptable_in_ms=time_to_wait_after_droptable_in_ms 
            )
        )
    await asyncio.gather(*tasks)
        
class ClientGenerator:

    def __init__(self, access_key, secret_key, token) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token
    
    def getNewClient(self):
        return boto3.client(
            'athena',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=token
         )
        


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('conf/conf.ini')

    # Ora per ogni query presente nel dict, lanciamola su athena con boto3
    queries_to_launch = prepareQueriesToLaunch(config)

    athena_config = configparser.ConfigParser()
    athena_config.read('conf/aws_data.ini')
    # print(config.sections())

    access_key = athena_config["DEFAULT"]["aws_access_key_id"]
    secret_key = athena_config["DEFAULT"]["aws_secret_access_key"]
    token = athena_config["DEFAULT"]["aws_session_token"]

    clientGen = ClientGenerator(
        access_key,
        secret_key,
        token
    )
    asyncio.run(execute_all_queries(clientGen, queries_to_launch))
    

    
# print(results)

