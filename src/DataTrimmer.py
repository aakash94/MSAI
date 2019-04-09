import time
import pandas as pd
import unicodedata
import unicodedata

import pandas as pd
import time

start_time = time.clock()

chunk_size = 1000000
tsv_path = "../res/data.tsv"
op_path = "../res/trimmed_data.tsv"

header_names = ["q_id", "question", "answer", "label", "a_id"]
trimmed_header_names = ["question", "answer", "label"]

encoding = 'utf-8'
start_time = time.time()
curr_time = time.time()

encoding = 'utf-8'


def clear_encoding(val):
    if isinstance(val, int):
        return val
    else:
        val = unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode()
    return val


file_read = 0

for chunk in pd.read_csv(tsv_path, sep='\t', chunksize=chunk_size, header=None, names=header_names, encoding=encoding):
    file_read += chunk.shape[0]

    del chunk['q_id']
    del chunk['a_id']

    chunk['question'].apply(lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore'))
    chunk['answer'].apply(lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore'))

    with open(op_path, 'a') as f:
        chunk.to_csv(f, sep='\t', encoding=encoding, index=False, header=False)

    print(file_read, "\t Done")
    time_taken = time.time() - curr_time
    print(file_read, " Done\t Time taken : ", (time_taken), "\t ETA : ",
          (((total_rows - file_read) / chunk_size) * time_taken))
    curr_time = time.clock()
print("--- %s seconds ---" % (time.time() - start_time))
