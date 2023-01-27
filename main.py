import configparser
config = configparser.ConfigParser()
conf = config.read('conf/conf.ini')

# Parte uno: ricava la lista dei file contenenti le query
file_with_list_of_query_to_launch = config['DEFAULT']['file_with_list_of_query_to_launch']

with open(file_with_list_of_query_to_launch, "r") as f:
    files_list = f.read().split('\n')

print(files_list)
# Per ogni file di query, leggere il contenuto e fare seguenti modifiche:
# - se pc: cambia now() con parametro cast(dataConf as date)
# - se health: sostituisci l3_rep_health.he_polizza con l3_rep_health_temp.he_polizza_temp
conf_date = conf['DEFAULT']['launch_date']
launch_date = f"cast('{conf_date}' as date)"

queries_to_launch = {}

for query_file in files_list:

    with open(query_file, "r") as f:
        query = f.read()

    query_format_1 = query.replace("now()", launch_date)

    if "he_" in query_file:
        query_format_1 = query_format_1.replace("l3_rep_health.he_polizza", "l3_rep_health_temp.he_polizza_temp")

    # Si memorizza il risultato del cambio del testo della query in un dict
    queries_to_launch[query_file] = query_format_1
    break

for k in queries_to_launch:
    query = queries_to_launch[k]
    
# Ora per ogni query presente nel dict, lanciamola su athena con boto3


