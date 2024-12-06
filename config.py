from datetime import timedelta

#+----------------+-----------------------------+-----------------------+-----------------------------+
#| TABLE_NAME     | COLUMN_NAME                 | REFERENCED_TABLE_NAME | REFERENCED_COLUMN_NAME      |
#+----------------+-----------------------------+-----------------------+-----------------------------+
#| order_payments | order_id                    | NULL                  | NULL                        |
#| order_payments | payment_sequential          | NULL                  | NULL                        |
#| order_payments | order_id                    | orders                | order_id                    |
#| sellers        | seller_id                   | NULL                  | NULL                        |
#| sellers        | seller_zip_code_prefix      | geolocation           | geolocation_zip_code_prefix |
#| products       | product_id                  | NULL                  | NULL                        |
#| order_items    | order_id                    | NULL                  | NULL                        |
#| order_items    | order_item_id               | NULL                  | NULL                        |
#| order_items    | order_id                    | orders                | order_id                    |
#| order_items    | product_id                  | products              | product_id                  |
#| order_reviews  | order_id                    | NULL                  | NULL                        |
#| order_reviews  | review_id                   | NULL                  | NULL                        |
#| order_reviews  | order_id                    | orders                | order_id                    |
#| orders         | order_id                    | NULL                  | NULL                        |
#| customers      | customer_id                 | NULL                  | NULL                        |
#| customers      | customer_zip_code_prefix    | geolocation           | geolocation_zip_code_prefix |
#| geolocation    | geolocation_zip_code_prefix | NULL                  | NULL                        |
#+----------------+-----------------------------+-----------------------+-----------------------------+

table_height = 900
page_height = 1030
show_all_edge = True
default_table = "orders"
accuracy = 0.0001
concurrency = 0.01
integer = 1
page_split = [1,2]
length = 10 # length of random string for savepoint
table_height = 900

timefstr = {}
timefstr['long'] = "%Y-%m-%d %H:%M:%S"
timefstr['short'] = "%Y-%m-%d"


time = {}
time['format']={}
time['format']['short'] = "YYYY-MM-DD"
time['format']['long'] = "YYYY-MM-DD HH:mm:ss"

time['step']={}
time['step']['second'] = timedelta(seconds=1)
time['step']['minute'] = timedelta(minutes=1)
time['step']['hour'] = timedelta(hours=1)
time['step']['day'] = timedelta(days=1)

#+----------------+-----------------------------+-----------------------+-----------------------------+
#| TABLE_NAME     | COLUMN_NAME                 | REFERENCED_TABLE_NAME | REFERENCED_COLUMN_NAME      |
#+----------------+-----------------------------+-----------------------+-----------------------------+
#| order_payments | order_id                    | orders                | order_id                    |
#| sellers        | seller_zip_code_prefix      | geolocation           | geolocation_zip_code_prefix |
#| order_items    | order_id                    | orders                | order_id                    |
#| order_items    | product_id                  | products              | product_id                  |
#| order_reviews  | order_id                    | orders                | order_id                    |
#| customers      | customer_zip_code_prefix    | geolocation           | geolocation_zip_code_prefix |
#+----------------+-----------------------------+-----------------------+-----------------------------+
