import sys
import os
import random

def createMatrixIndex(es_client, index_name, shards):
    if es_client.indices.exists(index_name):
        print("deleting '%s' index..." % (index_name))
        print(es_client.indices.delete(index = index_name, ignore=[400, 404]))

    request_body = {
        "settings" : {
            "number_of_shards": shards,
            "number_of_replicas": 1
        }
    }

    print("creating '%s' index..." % (index_name))
    print(es_client.indices.create(index = index_name, body = request_body))

def createRandomSparseMatrix(es_client, index_name, N, elem_range, shards, D):

    createMatrixIndex(es_client, index_name, shards)

    bulk_data = [] 

    num_of_elements = int(round(D * N**2))

    for elem_num in xrange(num_of_elements):
        
        # generate random row and column indices, and element value
        i = random.randint(1, N)
        j = random.randint(1, N)
        cell_val = random.randrange(-elem_range, elem_range, 1)

        # only store non-zero values
        if cell_val == 0:
            continue

        # use the ES bulk API
        bulk_data.append({
            "index": {
                "_index": index_name, 
                "_type": 'elem', 
                "_id": '%s-%s' % (i,j)
            }
        })
        bulk_data.append({
            'row': i,
            'col': j,
            'val': cell_val
        })

        if len(bulk_data) > 10000:
            res = es_client.bulk(index=index_name,body=bulk_data,refresh=True)
            bulk_data = []
        
    if len(bulk_data) > 0:
        res = es_client.bulk(index=index_name,body=bulk_data,refresh=True)

if __name__ == "__main__":

    N = 100