import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
        user = os.getenv('SF_USER'), # USER and USERNAME are already used
        password = os.getenv('PASSWORD'),
        account = os.getenv('ACCOUNT'),
        warehouse = os.getenv('WAREHOUSE'),
        database = os.getenv('DATABASE'),
        schema = os.getenv('SCHEMA')
        )
cur = conn.cursor()

try:
    cur.execute('''
        select lag(id, 1) over (order by id) prev_id, id
        from items
        qualify id - prev_id > 1
        order by 1;
    ''')

    missing_ids = []
    for (start_id, end_id) in cur:
        missing_ids += list(range(start_id + 1, end_id))

    missing_urls = []
    for missing_id in missing_ids:
        url = f'https://hacker-news.firebaseio.com/v0/item/{missing_id}.json'
        missing_urls.append(url)

    if len(missing_urls) > 0:
        with open('missing_urls', 'w') as f:
            content = '\n'.join(x for x in missing_urls)
            f.write(content + '\n')
finally:
    cur.close()
    conn.close()
