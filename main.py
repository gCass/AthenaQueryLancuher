import configparser
import boto3
import json
import time

def printIfDebug(debugFlag, value):
    if debugFlag:
        print(value)


def has_query_succeeded(client, execution_id, max_execution = 30):
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
        time.sleep(30)

    return False, response

def get_query_results(client, execution_id):
    response = client.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet']['Rows']
    return results

def prepareQueriesToLaunch(config):

    # Parte uno: ricava la lista dei file contenenti le query
    file_with_list_of_query_to_launch = config['DEFAULT']['file_with_list_of_query_to_launch']

    with open(file_with_list_of_query_to_launch, "r") as f:
        files_list = f.read().split('\n')

    conf_date = config['DEFAULT']['launch_date']
    launch_date = f"cast('{conf_date}' as date)"

    queries_to_launch = {}

    for query_file in files_list:

        # TODO: aggiungere check sull'esistenza del percorso. Se esiste un errore intte
        with open(query_file, "r") as f:
            query = f.read()

        query_format_1 = query.replace("now()", launch_date)

        if "he_" in query_file:
            query_format_1 = query_format_1.replace("l3_rep_health.he_polizza", "l3_rep_health_temp.he_polizza_temp")

        queries_to_launch[query_file] = query_format_1

    return queries_to_launch

def execute_all_queries(client, queries_to_launch, debugPrint = False):
    results = {}
    i = 0
    for k in queries_to_launch:
        query = queries_to_launch[k]
        if debugPrint:
            print(f"i={0}", k)
        response = client.start_query_execution(QueryString=query)
        query_exec_id = response["QueryExecutionId"]
        
        printIfDebug(debugPrint, f"Query id {query_exec_id}")

        query_status, status_reponse = has_query_succeeded(client, execution_id=query_exec_id)
        if query_status:
            result_i = "success"
        else:
            result_i = f"failure.\n {status_reponse['QueryExecution']['Status']['StateChangeReason']}"
        
        printIfDebug(debugPrint, f"Result: {result_i}")

        results[k] = result_i
        p = k.split("\\")[-1].split(".")[0]
        json_formatted_str = json.dumps(str(status_reponse), indent=2)
        with open(f"results/{p}", "w") as f:
            f.write(json_formatted_str)
        
        printIfDebug(debugPrint, "========\n\n\n")
        i += 1



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

    client = boto3.client(
        'athena',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=token
    )

    execute_all_queries(client, queries_to_launch, debugPrint=True)


    
# print(results)

