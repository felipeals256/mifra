import psycopg2
import pymssql
import settings

class Conexion():

	objeto=None
	conn = None
	cursor=None
	encabezado=None

	def __init__(self,objeto=None):

		self.objeto = objeto
		self.getConexion()

	def getConexion(self):


		con="dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASS}'".format( DB_NAME=settings.DB_NAME,  DB_USER=settings.DB_USER,  DB_HOST=settings.DB_HOST,  DB_PASS=settings.DB_PASS)
		#Si el objeto tiene otra conexion, asignamos esa conexion
		tipo_con = Conexion.getTipoConexion(self.objeto)

		if tipo_con != "postgres":
			con = getattr(self.objeto, 'Meta').db_conexion 

		if(self.conn):
			return self.conn

		#try:

		if tipo_con == "postgres" : 
			self.conn = psycopg2.connect(con)

		if tipo_con == "sqlserver" : 
			#self.conn = pymssql.connect(con)
			self.conn=pymssql.connect(server=con['server'], user=con['user'], password=con['password'], database=con['database'])

		return self.conn 

		#except:
		#	print ("Problemas con {}".format(tipo_con))
		#	exit()


	def close(self):
		self.cursor.close()
		self.conn.close()


	def get(self,sql):

		if not sql:
			return None

		self.cursor = self.getConexion().cursor()
		self.cursor.execute(sql)
		columns = self.cursor.description
		self.encabezado=[]
		for aux in columns:#recorre solo las culmnas
			self.encabezado.append(aux[0])
		
		filas = self.cursor.fetchall()
		self.conn.commit() #hace el commit

		return filas

	def execute(self,sql):

		if not sql:
			return None

		self.cursor = self.getConexion().cursor()
		self.cursor.execute(sql)
		self.conn.commit()



	def getTipoConexion(objeto):
		tipo = "postgres"
		try:
			con = getattr(objeto, 'Meta').db_conexion

			if not 'db_tipo' in con:
				print("ERROR: En la clase Meta del objeto debe agregar el tipo de conección con el campo 'db_conexion' que puede ser 'postgres' o 'sqlserver' ")
				exit()

			tipo = con['db_tipo']
		except:
			pass
			
		return tipo


	class reg(object):

		def __init__(self, cursor, row):
			for (attr, val) in zip((d[0] for d in cursor.description), row) :
				setattr(self, attr, val)