import numpy as np
import pandas as pd
import psycopg2
import math
from faker import Faker
import uuid
from ipywidgets import IntProgress
from IPython.display import display
from datetime import datetime, timedelta
import multiprocessing as mp
from pathos.multiprocessing import ProcessingPool as Pool
from sqlalchemy import create_engine
from itertools import repeat
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# Database params
POSTGRES_HOST='database'
POSTGRES_PASSWORD='password'
POSTGRES_USER='user'
POSTGRES_DB='db'

# Define number of clients to be generated
N_CLIENTS = 500

# Start date of the company is 2021/01/01
START_DATE = datetime.fromisoformat('2021-01-01 00:00:00')

# Number of processors for multi-processing (minues 1 to keep one process for the system and UI)
MAX_PROCESSES = mp.cpu_count()-1

# Get the connection to the database
def get_db_conn():
    conn = psycopg2.connect(f"dbname='{POSTGRES_DB}' user='{POSTGRES_USER}' host='{POSTGRES_HOST}' password='{POSTGRES_PASSWORD}'")
    conn.autocommit = True
    return(conn)

# Create the tables in the database and if exists drop them
def create_db_tables():
    # Connect to the database
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS sessions CASCADE;")
    cur.execute("DROP TABLE IF EXISTS companies CASCADE;")
    cur.execute("DROP TABLE IF EXISTS subscriptions CASCADE;")

    cur.execute(""" CREATE TABLE IF NOT EXISTS subscriptions (
                        sub_id VARCHAR(255) PRIMARY KEY,
                        sub_price INT NOT NULL
                    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS companies ( 
                        company_id VARCHAR(255) PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        company_size VARCHAR(255) NOT NULL,
                        company_created_at TIMESTAMP NOT NULL,
                        FOREIGN KEY(company_size) REFERENCES subscriptions(sub_id)
                    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS sessions (
                        session_id VARCHAR(255) PRIMARY KEY,
                        session_company_id VARCHAR(255) NOT NULL,
                        session_created_at TIMESTAMP NOT NULL,
                        session_duration FLOAT NOT NULL,
                        FOREIGN KEY(session_company_id) REFERENCES companies(company_id)
                    )""")

    cur.close()

# Create random sessions for each client and month
def generate_random_sessions(sub_companies, start_date):
    
    import pandas as pd
    import numpy as np
    import uuid
    from datetime import timedelta
    from dateutil.relativedelta import relativedelta
    from calendar import monthrange
    
    sub_sessions = pd.DataFrame(columns = ["session_id","session_company_id","session_created_at","session_duration"])
    
    for index, row in sub_companies.iterrows():
    
        client_active = True
        month = 0
        while client_active and (month<12):
            
            sessions_per_company = {'small': 5 + (([-1,1][np.random.randint(2)]) * np.random.randint(low=0, high=6)), 
                                    'large': 10 + (([-1,1][np.random.randint(2)]) * np.random.randint(low=0, high=11))}
            
            number_sessions = sessions_per_company.get(row.company_size)
            
            # Check if the number of sessions is 0
            if(number_sessions == 0):
                client_active = False
            else:
                for session in range(number_sessions):
                    sub_sessions = sub_sessions.append({'session_id': uuid.uuid4(), 
                                                'session_company_id': row.company_id,
                                                'session_created_at': start_date + relativedelta(months=month) + timedelta(seconds=np.random.randint(monthrange(start_date.year, month+1)[1]*24*60*60)),
                                                'session_duration': abs(np.random.normal(15,5))
                                            },ignore_index=True)
                
            month+=1
    
    return(sub_sessions)

# Generate fake data for each table in the database based on the requirements specified
def generate_fake_data():

    # Init tables
    companies = pd.DataFrame(columns = ["company_id","company_name","company_size","company_created_at"])
    subscriptions = pd.DataFrame([{"sub_id":"small","sub_price":19},{"sub_id":"large","sub_price":99}],columns = ["sub_id","sub_price"])
    sessions = pd.DataFrame(columns = ["session_id","session_company_id","session_created_at","session_duration"])

    # Init faker to generate random company names
    faker = Faker()

    # Create random clients
    for n in range(N_CLIENTS):
                
        # Creation date of the client is within the first month of the start up
        created_at = START_DATE + timedelta(seconds= np.random.randint(monthrange(START_DATE.year, START_DATE.month)[1]*24*60*60))

        companies = companies.append({'company_id': uuid.uuid4(), 
                                    'company_name': faker.company(),
                                    'company_size': np.random.choice(["small","large"], p=[0.7,0.3]),
                                    'company_created_at': created_at
                                    },ignore_index=True)

    # Create the multiprocessing pool to generate random sessions in parallel
    pool = Pool(MAX_PROCESSES)
        
    companies_split = np.array_split(companies, MAX_PROCESSES, axis=0)

    # Process the DataFrame by mapping function to each df across the pool
    sessions = pd.DataFrame(np.vstack(pool.map(generate_random_sessions, companies_split, repeat(START_DATE))), 
                            columns=["session_id","session_company_id","session_created_at","session_duration"])

    # Close down the pool and join
    pool.close()
    pool.join()
    pool.clear()
    
    return(companies, subscriptions, sessions)

# Upload all the data generated into the database
def upload_to_db(companies, subscriptions, sessions):
    # Connect to the database
    db = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}")
    conn = db.connect()
    conn.autocommit = True

    subscriptions.to_sql('subscriptions', con=conn, if_exists='append', index=False)
    companies.to_sql('companies', con=conn, if_exists='append', index=False)
    sessions.to_sql('sessions', con=conn, if_exists='append', index=False)

    conn.close()

# Get the number of rows from a table in the database
def get_number_rows(table):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    result = cur.fetchone()
    return(result[0])

# Check the number of rows in the DB match with the data we have uploaded
def check_rows(df, table):
    rows_in_memory = len(df)
    rows_in_database = get_number_rows(table)
    assert rows_in_memory == rows_in_database, f"The number of rows in the table '{table}' is not correct. On the database there are {rows_in_database} rows and it should be {rows_in_memory}."


if __name__ == '__main__':
    
    print(f"[{datetime.now()}] START of the program")
    
    # -- Check DB connection
    try:
        get_db_conn()
        print(f"[{datetime.now()}] Connected to the database!")
    except:
        print(f"[{datetime.now()}] Error while connecting to the database!")
        raise SystemExit
    
    # -- Create the tables in the database and remove them if they exist
    print(f"[{datetime.now()}] Start creation of the tables in the database")
    create_db_tables()
    
    # -- Generate fake data for each table in the DB
    print(f"[{datetime.now()}] Start generation of fake data")
    companies, subscriptions, sessions = generate_fake_data()
    
    # -- Upload the data to the database --
    print(f"[{datetime.now()}] Start uploading fake data to the database")
    upload_to_db(companies, subscriptions, sessions)
    
    # -- Check if the data in the database is correct --
    print(f"[{datetime.now()}] Start validation of data in database")
    check_rows(companies, 'companies')
    check_rows(sessions, 'sessions')
    check_rows(subscriptions, 'subscriptions')
    
    print(f"[{datetime.now()}] END of the program")
