from datetime import timedelta
import traceback
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.n1ql import QueryScanConsistency
from couchbase.management.collections import CollectionSpec
import time


endpoint = "couchbases://cb.n3hga1y2tjerhxgq.cloud.couchbase.com"
username = "lab"
password = "LabaLabaaa2398!"
bucket_name = "AuditDB"
scope_name = "default_scope"
collection_name = "testing"

auth = PasswordAuthenticator(username, password)
options = ClusterOptions(auth)

try:
    cluster = Cluster(endpoint, options)
    cluster.wait_until_ready(timedelta(seconds=5))
    #print("Успешно подключено к кластеру")

    # Connect to the bucket
    cb = cluster.bucket(bucket_name)
    #print(f"Бакет '{bucket_name}' подключен успешно")

    scope = cb.scope(scope_name)
    #print(f"Скоуп '{scope_name}' подключен успешно")

    collection_manager = cb.collections()

    try:
        collection_manager.create_collection(scope_name, collection_name, None)
        print(f"Коллекция '{collection_name}' успешно создана!")
    except Exception as e:
        print(f"Ошибка при создании коллекции '{collection_name}' : {e}")
    print("=========================")
    collection = scope.collection(collection_name)
    print("Вставляем записи...")
    data = {
        "operation::001": {"id": "008", "type": "Чтение последовательного файла", "resource_cost": 200},
        "operation::002": {"id": "009", "type": "Запись в последовательный файл", "resource_cost": 300},
        "operation::003": {"id": "010", "type": "Обращение к внешним устройствам", "resource_cost": 150},
    }

    for key, value in data.items():
        collection.upsert(key, value)

    print("Записи успешно вставлены!")
    print("=========================")

    query = f"SELECT * FROM {bucket_name}.{scope_name}.{collection_name}"
    result = cluster.query(query, QueryOptions())

    print("Содержимое коллекции:")
    for row in result:
        print(row)
    print("=========================")

    new_resource_cost = 150
    target_id = "008"

    print("Обновляем запись, где id = 008: меняем resource_cost на 150...")
    query = f"""
    UPDATE {bucket_name}.{scope_name}.{collection_name} AS t
    SET t.resource_cost = {new_resource_cost}
    WHERE t.id = "{target_id}"
    RETURNING t.id, t.resource_cost
    """
    result = cluster.query(query, QueryOptions())
    for row in result:
        print(f"Обновленная запись: {row}")
    print("=========================")

    print("Удаляем запись, где id = 009...")
    query = f"""
    SELECT META().id FROM {bucket_name}.{scope_name}.{collection_name}
    WHERE id = "009"
    """
    result = cluster.query(query, QueryOptions())

    collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name)

    for row in result:
        doc_key = row["id"]  
        collection.remove(doc_key)  

    print("Документ с id = '009' удален!")
    query = f"SELECT * FROM {bucket_name}.{scope_name}.{collection_name}"
    result = cluster.query(query, QueryOptions())

    print("Содержимое коллекции:")
    for row in result:
        print(row)
    print("=========================")

    query = f"SELECT META().id FROM {bucket_name}.{scope_name}.{collection_name}"
    result = cluster.query(query, QueryOptions())

    collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name)

    for row in result:
        doc_id = row["id"]
        collection.remove(doc_id)

    print("Все документы в коллекции удалены!")

except Exception as e:
    print("Критическая ошибка:")
    traceback.print_exc()





