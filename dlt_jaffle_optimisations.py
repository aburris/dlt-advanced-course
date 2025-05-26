
import os
os.environ['RUNTIME__LOG_LEVEL'] = 'WARNING'

import dlt
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator, PageNumberPaginator
from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo

import time


@dlt.source()
def jaffle_shop(parallelized,start_date):
    
    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=HeaderLinkPaginator(
           links_next_key="next" # see 'curl -s -I  https://jaffle-shop.scalevector.ai/api/v1/orders | grep link'
        )
    )

    @dlt.resource(table_name="orders",parallelized=parallelized)
    def orders():
        #print("processing orders")
        for page in client.paginate("/orders", params={"start_date": start_date}):
            yield page
   

    @dlt.resource(table_name="customers",parallelized=parallelized)
    def customers():
        #print("processing customers")
        for page in client.paginate("/customers", params={"start_date": start_date}):
            yield page

    @dlt.resource(table_name="products",parallelized=parallelized)
    def products():
        #print("processing products")
        for page in client.paginate("/products", params={"start_date": start_date}):
            yield page

    # return orders, customers, products
    return customers, products

os.environ['EXTRACT__WORKERS'] = '3'
os.environ['NORMALIZE__WORKERS'] = '1'
os.environ['LOAD__WORKERS'] = '3'
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '1000'
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'

START_DATE = "2017-01-01"
PARALLELIZED = True

totalruntime=0
for i in range(5):

    pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop",
    destination="duckdb",
    dataset_name="jaffle_test",
    )

    starttime = time.time()


    load_info = pipeline.run(jaffle_shop(parallelized=PARALLELIZED, start_date=START_DATE))

    runtime = round(time.time()-starttime,1)
    totalruntime += runtime
    #print(f"run #{i} took {runtime} sec")
    #print(pipeline.dataset().row_counts().df())

print("#"*80)
print(f"total runtime {round(totalruntime,2)} and average runtime {round(totalruntime/5,2)}")

# without fiddling arount
# no "optimasation" # total runtime 10.0 and average runtime 2.0

# 5 runs each with parralized false/true without orders
# parallelized = False # total runtime 18.0 and average runtime 3.6
# parallelized = True # total runtime 18.8 and average runtime 3.76
# -> keeping True

# 5 runs each with diffent extract workers
# os.environ['EXTRACT__WORKERS'] = '1' # total runtime 24.1 and average runtime 4.82
# os.environ['EXTRACT__WORKERS'] = '2' # total runtime 8.8 and average runtime 1.76
# os.environ['EXTRACT__WORKERS'] = '3' # total runtime 8.4 and average runtime 1.68
# os.environ['EXTRACT__WORKERS'] = '5' # total runtime 9.6 and average runtime 1.92
# os.environ['EXTRACT__WORKERS'] = '10' # total runtime 9.6 and average runtime 1.92
# -> keeping 3 workers

# 5 runs each with diffent normalize workers
# os.environ['NORMALIZE__WORKERS'] = '1' # total runtime 10.1 and average runtime 2.02
# os.environ['NORMALIZE__WORKERS'] = '3' # total runtime 12.1 and average runtime 2.42
# os.environ['NORMALIZE__WORKERS'] = '10' # total runtime 12.7 and average runtime 2.54
# -> keeping 1 worker

# 5 runs each with diffent load workers
# os.environ['LOAD__WORKERS'] = '1' # total runtime 20.4 and average runtime 4.08
# os.environ['LOAD__WORKERS'] = '2' # total runtime 17.2 and average runtime 3.44
# os.environ['LOAD__WORKERS'] = '3' # total runtime 9.8 and average runtime 1.96
# os.environ['LOAD__WORKERS'] = '5' # total runtime 12.3 and average runtime 2.46
# os.environ['LOAD__WORKERS'] = '10' # total runtime 13.1 and average runtime 2.62
# os.environ['LOAD__WORKERS'] = '20' # total runtime 19.8 and average runtime 3.96
# -> keeping 3 workers

# 5 runs each with diffent data writer max items
# os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '1' # total runtime 89.2 and average runtime 17.84
# os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10' # total runtime 41.7 and average runtime 8.34
# os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '100' # total runtime 11.1 and average runtime 2.22
# os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '1000' #  total runtime 8.9 and average runtime 1.78
# os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000' # total runtime 10.1 and average runtime 2.02
# -> keeping 1000 max items

# 5 runs each with diffent normalizer max items
# os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '10' # total runtime 11.6 and average runtime 2.32
# os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '100' # total runtime 12.0 and average runtime 2.4
# os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000' # total runtime 10.9 and average runtime 2.18

# summary
# dlt is running good as it, not worth fiddling arount (at least on this machine with this jobsize)