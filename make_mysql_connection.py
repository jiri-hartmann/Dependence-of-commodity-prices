# -*- coding: utf-8 -*-
""" Open database connection and return it.
"""
__author__      = "Jiří Hartmann"
__email__ = "jiri.hartmann@gmail.com"

def make_mysql_connection(user="root", db_name="ades", db_server='localhost'):

    import sqlalchemy
    import keyring

    service = "MariaDB" # name of service in Windows valet

    try:
        password = keyring.get_password(service, user)
        if not password:
            password = input(f'Input password to system {service} for user {user}: ')
            keyring.set_password(service, user, password)

        conn_string = f"mysql+pymysql://{user}:{password}@{db_server}/{db_name}"
        alchemy_conn = sqlalchemy.create_engine(conn_string)
        return alchemy_conn
 
    except:
        return None

