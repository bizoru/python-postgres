import datetime

# Esta es la libreria que necesitamos para conectar con postgres
import psycopg2
import psycopg2.extras
import sys

args = sys.argv

if len(args) < 2:
    print "Debe ejecutar el programa como python dbhandler.py <consulta>"
    print "Ejemplo: python dphandler.py 'select * from alumnos;'"
    sys.exit(0)

""" 
Los siguientes son los parametros para conectar a la base de datos.
db_server: ip o nombre del servidor postgres
db_name: nombre de la base de datos
db_username: nombre del usuario de la base de datos
db_password: contrasena del usuario para la base de datos
"""
db_server = "localhost"
db_name = ""
db_username = ""
db_password = ""


"""
Clase que maneja las conexiones y las consultas con la Base de Datos
"""
class DBHandler:
    def __init__(self):
        try:
            # Conexion a la base de datos, se hace un try por que si no responde el servidor ejecutara el error en el except
            self.conn = psycopg2.connect(
                "dbname='{}' user='{}' host='{}' password='{}'".format(db_name, db_username, db_server, db_password))
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            print e
            print "No se pudo conectar a la base de datos"
    
    """
    Con este metodo vamos a hacer consultas que no retornen resultados, como actualzaciones,borrados e inserciones
    """
    def query(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print e
            self.conn.rollback()
            print "No se pudo correr la consulta {}".format(query)

    """
    Con este metodo vamos a traer resultados de una tabla a traves de una consulta
    Este nos va a mostrar la consulta que hemos hecho y la cantidad de registros encontrados
    en caso que no hayan registros nos desplegara el mensaje de no se econtraron registros
    """

    def fetch_results(self, query):

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            column_names = []
            for key in self.cursor.description:
                column_names.append(key.name)
            if len(rows) > 0:
                print "Consulta: '{}' Total Registros: {}".format(query,str(len(rows)))

                print ",".join(column_names)
                for item in rows:
                    print ",".join([str(k) for k in item])
            else:
                print "No se encontraron registros"
        except Exception as e:
            print e
            self.conn.rollback()
            print "No se pudo correr la consulta {}".format(query)

    def print_results(results):
        if len(results) > 0:
            pass 

"""
Debemos instanciar DBHandler para utilizar sus metodos para hacer consultas
"""
db = DBHandler()
consulta = args[1]
db.fetch_results(consulta)


