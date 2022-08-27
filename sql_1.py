import psycopg2
from psycopg2 import OperationalError

import config
from config import username_3, password_3, danabase_3



def create_connection():
    try:
        conn = psycopg2.connect(
            database=config.danabase_3,
            user=config.username_3,
            password=config.password_3)
        cur = conn.cursor()
        print("Connection to PostgreSQL DB successful")
        return cur, conn

    except OperationalError as err:
        print(f"The error '{err}' occurred")



def create_db(cur, conn, query):

    try:
        cur.execute(query)
        conn.commit()
        print("Table created")
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


create_phones_table = """
    CREATE TABLE IF NOT EXISTS phones(
    phone_id INTEGER PRIMARY KEY,
    client_id INTEGER REFERENCES clients(client_id),
    phone_number VARCHAR(40) UNIQUE
    )
    """


def add_client(cur, conn):
    for client in in_clients:
        try:
            insert_table_clients = '''
                INSERT INTO clients (client_id, name, surname, email)
                VALUES (%s, '%s', '%s', '%s');
                ''' % (client[0], client[1], client[2], client[3])
            create_db(cur, conn, insert_table_clients)
            conn.commit()
            print("Table Clients inserted")
        except OperationalError as err:
            print(f"The error '{err}' occurred")

in_clients = [
    (1,'Ivan', 'Ivanov', 'IvIvanov@mail.org.ru'),
    (2, 'Oleg', 'Petrov', 'Oleg1petrov@mail.ru'),
    (3, 'Alex', 'Markov', 'AlexMarkov@gmail.com'),
 ]


def add_phones(cur, conn):
    for phone in in_phones:
        try:
            insert_table_phones = '''
                INSERT INTO phones (phone_id, client_id, phone_number)
                VALUES (%s, '%s', '%s');
                ''' % (phone[0], phone[1], phone[2])
            create_db(cur, conn, insert_table_phones)
            conn.commit()
            print("Table Phones inserted")
        except OperationalError as err:
            print(f"The error '{err}' occurred")
in_phones = [
    (1, 1, '+76051223322'),
    (2, 1, '+78225122333'),
    (3, 2, '+79213338899'),
    (4, 3, '+79214446677'),
 ]

def change_client(cur,conn, ch_client):
    ch_client = ch_client[0]
    in_change_client = '''
            UPDATE clients 
            SET %s = '%s'
            WHERE client_id = %s
            ''' % (ch_client[1], ch_client[2], ch_client[0])
    cur.execute(in_change_client)
    conn.commit()

ch_client = [
    (3, 'name', 'Nero'),
]


def del_phone(cur, conn, client_id):
    query = '''
            DELETE FROM phones
            WHERE client_id = %s
            ''' %(client_id)
    create_db(cur, conn, query)


def del_client(cur, conn, client_id):
    query_1 = '''
        DELETE FROM phones
        WHERE client_id = %s;
        ''' % (client_id)
    create_db(cur, conn, query_1)
    conn.commit()
    query_2 = '''
        DELETE FROM clients
        WHERE client_id = %s;
        ''' % (client_id)
    create_db(cur, conn, query_2)
    conn.commit()


def searh_client(cur, sear_clie):
    sear_clie = sear_clie[0]

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


if __name__ == '__main__':
    cur = create_connection()[0]
    conn = create_connection()[1]
    create_db(conn.cursor(), conn, create_clients_table)
    create_db(conn.cursor(), conn, create_phones_table)
    add_client(conn.cursor(), conn)
    add_phones(conn.cursor(), conn)
    change_client(conn.cursor(), conn, ch_client)
    del_phone(conn.cursor(), conn, 3)
    searh_client(conn.cursor(), sear_clie)



conn.close()
