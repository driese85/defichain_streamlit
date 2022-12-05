"""PostgreSQL helper functions"""
import sys
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError


def show_psycopg2_exception(err):
    """ Define a function that handles and parses psycopg2 exceptions """
    # get details about the exception
    err_type, _, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("psycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def connect(conn_params_dic):
    """ Define a connect function for PostgreSQL database server """
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successfully..................")

    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
    return conn
