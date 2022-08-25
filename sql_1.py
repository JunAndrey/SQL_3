import psycopg2
from psycopg2 import OperationalError



def create_connection(db_name, db_user, db_password):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as err:
        print(f"The error '{err}' occurred")
    return conn


conn = create_connection("clients_db", "postgres", "...")




with conn.cursor() as cur:
    cur.execute("""
                DROP TABLE phones;
                DROP TABLE clients;
                """)


def create_db(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        print("Query executed successfully")
    except OperationalError as err:
        print(f"The error '{err}' occurred")


create_clients_table = """
    CREATE TABLE IF NOT EXISTS clients(
    client_id INTEGER PRIMARY KEY,
    name VARCHAR(40) NOT NULL,
    surname VARCHAR(40) UNIQUE,
    email VARCHAR(40) NOT NULL
    )
    """

create_db(conn, create_clients_table)
create_phones_table = """
    CREATE TABLE IF NOT EXISTS phones(
    phone_id INTEGER PRIMARY KEY,
    client_id INTEGER REFERENCES clients(client_id),
    phone_number VARCHAR(40) UNIQUE
    )
    """

create_db(conn, create_phones_table)

def add_client(conn, in_clients):
    for client in in_clients:
        query = '''
                    INSERT INTO clients (client_id, name, surname, email)
                    VALUES (%s, '%s', '%s', '%s');
                    ''' % (client[0], client[1], client[2], client[3])
        create_db(conn, query)

in_clients = [
    (1,'Ivan', 'Ivanov', 'IvIvanov@mail.org.ru'),
    (2, 'Oleg', 'Petrov', 'Oleg1petrov@mail.ru'),
    (3, 'Alex', 'Markov', 'AlexMarkov@gmail.com')
 ]
add_client(conn, in_clients)

def add_phones(conn, in_phones):
    for phone in in_phones:
        query = '''
                    INSERT INTO phones (phone_id, client_id, phone_number)
                    VALUES (%s, '%s', '%s');
                    ''' % (phone[0], phone[1], phone[2])
        create_db(conn, query)

in_phones = [
    (1, 1, '+76051223322'),
    (2, 1, '+78225122333'),
    (3, 2, '+79213338899'),
    (4, 3, '+79214446677'),
 ]
add_phones(conn, in_phones)

def change_client(conn, ch_client):
    ch_client = ch_client[0]
    query = '''
            UPDATE clients 
            SET %s = '%s'
            WHERE client_id = %s
            ''' % (ch_client[1], ch_client[2], ch_client[0])
    create_db(conn, query)

ch_client = [
    (3, 'name', 'Nero'),
]
change_client(conn, ch_client)

def del_phone(conn, client_id):
    query = '''
            DELETE FROM phones
            WHERE client_id = %s
            ''' %(client_id)
    create_db(conn, query)

del_phone(conn, 3)

def del_client(conn, client_id):
    cur = conn.cursor()
    cur.execute('''
        
        DELETE FROM phones
        WHERE client_id = %s
        ''' % (client_id))
    conn.commit()
    query = '''
        DELETE FROM clients
        WHERE client_id = %s
        ''' % (client_id)
    create_db(conn, query)
    conn.commit()

del_client(conn, 1)

def searh_client(conn, cur, sear_clie):
    sear_clie = sear_clie[0]
    cur = conn.cursor()

    try:
        cur.execute('''
            SELECT client_id,name FROM clients cls
            JOIN phones phs USING (client_id)
            WHERE %s = '%s';
            ''' % (sear_clie[0], sear_clie[1]))

        print("Query executed successfully!!!")
    except OperationalError as err:
        print(f"The error '{err}' occurred")
    conn.commit()
    print(cur.fetchall())
sear_clie = [
     ('surname', 'Petrov'),
 ]
searh_client(conn, cur, sear_clie)

conn.close()
