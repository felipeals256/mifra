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

	def getCon(con_params):

		conn = None
		if con_params['db_tipo'] == "postgres" : 

			con="dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASS}' port='{DB_PORT}'".format( DB_NAME=con_params['database'],
			  																										DB_USER=con_params['user'],
			  																										DB_HOST=con_params['server'],
			  																										DB_PASS=con_params['password'],
			  																										DB_PORT=con_params['port'],
			  																										)
			conn = psycopg2.connect(con)

		if con_params['db_tipo'] == "sqlserver" : 
			conn=pymssql.connect(server=con_params['server'], user=con_params['user'], password=con_params['password'], database=con_params['database'])

		return conn



	def getConexion(self):


		
		#Si el objeto tiene otra conexion, asignamos esa conexion
		con_params = Conexion.get_db_conexion(self.objeto)

		if(self.conn):
			return self.conn

		#try:
		self.conn = Conexion.getCon(con_params)
	
		return self.conn 


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

	def set_cursor(self):
		self.cursor = self.getConexion().cursor()

	def get_db_conexion(objeto):

		port = 5432
		try:
			port =  settings.DB_PORT
		except:
			pass

		conect={'server':settings.DB_HOST,
			'user':settings.DB_USER,
			'password':settings.DB_PASS,
			'database':settings.DB_NAME,
			'db_tipo':'postgres',
			'port':port,
			}
		
		try:
			con = getattr(objeto, 'Meta').db_conexion

			if not 'server' in con:
				print("ERROR: En la clase Meta del objeto debe agregar el host con el campo 'server'")
				exit()
			if not 'user' in con:
				print("ERROR: En la clase Meta del objeto debe agregar el campo 'user'")
				exit()
			if not 'password' in con:
				print("ERROR: En la clase Meta del objeto debe agregar la contraseña de la base de datos en el campo 'password'")
				exit()
			if not 'database' in con:
				print("ERROR: En la clase Meta del objeto debe agregar el nombre de la base de datos en el campo 'database'")
				exit()
			if not 'db_tipo' in con:
				print("ERROR: En la clase Meta del objeto debe agregar el tipo de conección con el campo 'db_conexion' que puede ser 'postgres' o 'sqlserver' ")
				exit()

			conect['server']  =con['server']
			conect['user']    =con['user']
			conect['password']=con['password']
			conect['database']=con['database']
			conect['db_tipo'] =con['db_tipo']

			if not 'port' in con:
				if con['db_tipo']=="postgres":
					conect['port']=5432
				if con['db_tipo']=="sqlserver":
					conect['port']=1433
			else:
				conect['port'] =con['port']
		except:
			pass
	
		return conect

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