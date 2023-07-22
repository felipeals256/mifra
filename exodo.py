import settings
from mifra.core.conexion import Conexion
from mifra.core.core import Core
from mifra.core.utility import Utility

import psycopg2

class Procesar(object):

	data = []
	cursor=None
	conn = None
	object_migarte=None

	def __init__(self,**kwargs):

		self.conn = kwargs['conn']
		self.cursor = kwargs['cursor']
		self.object_migarte = kwargs['object_migarte']


	"""
		Obtiene los id de las dependencias
		la idea es generar un diccionario con el campo clave que contiene el id
	"""
	def getDependencias(**kwargs):
		aux_conn = psycopg2.connect("dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' password='{DB_PASS}'".format( 
				DB_NAME=settings.DB_NAME,  
				DB_USER=settings.DB_USER,  
				DB_HOST=settings.DB_HOST,  
				DB_PASS=settings.DB_PASS
			)
		)

		aux_cursor = aux_conn.cursor()
		
		dependencias = {}
		for kwarg in kwargs:
			#busca los kwargs que sean una tupla
			if str(type(kwargs[kwarg])) == "<class 'tuple'>":
				dependencias[ kwargs[kwarg][0] ]={}
				campo = str(kwargs[kwarg][1]).split(".")
				aux_cursor.execute("select {campo},id from {tabla}".format(campo=campo[1],tabla=campo[0]))
				for row in aux_cursor.fetchall():
					dependencias[ kwargs[kwarg][0] ][ str(row[0]).upper().strip() ]=str(row[1]).upper().strip()

		aux_cursor.close()
		aux_conn.close()


		"""
			el campo del objeto base mas el codigo y su id del objeto referencia
			{'CODCLI': {'20091100029': 1, 'I000118664793': 2},'CODCARR': {'101C': 1, '101P': 2}
		"""
		return dependencias


	def format(self,**kwargs):

		columnas,_valores = self.validarAtributos(**kwargs)
		dependencias = Procesar.getDependencias(**kwargs)
		#print(dependencias['PERSONA_ID'])
		#exit();
		tabla_name =  Core.getTabla(self.object_migarte)

	

		i=0
		encabezado={}
		for col in self.cursor.description:
			encabezado[col[0]]=i
			i+=1

		inserts=""
		#print("creando script....")
		for row in self.cursor.fetchall():
			valores=_valores
			for attr in kwargs:
				if str(type(kwargs[attr])) == "<class 'tuple'>":
					try:
						valores=valores.replace("%"+attr+"%",   str(dependencias[  kwargs[attr][0]  ][ str(row[encabezado[kwargs[attr][0] ]]).upper().strip() ])    )

					except Exception as e:
						valores=valores.replace("%"+attr+"%",   str("None")    )
						print( "No existe "+str(row[encabezado[kwargs[attr][0] ]]).upper()+" en "+kwargs[attr][1] )
						exit()
				else:

					if not kwargs[attr] or len(str(row[encabezado[kwargs[attr]]]).replace(" ","")) == 0 :
						valores=valores.replace("%"+attr+"%", str('None') )
					else: 
						valores=valores.replace("%"+attr+"%", str(row[encabezado[kwargs[attr]]]).replace("'","`") )

			insert = """INSERT INTO {tabla_name} ({columnas}) VALUES ({valores}) ||;|| """.format(tabla_name=tabla_name,columnas=columnas,valores=valores)

			

			inserts += " "+insert.replace("'None'","NULL").replace("None","NULL")

		self.cursor.close()
		self.conn.close()



		return inserts.split("||;||")


	def validarAtributos(self,**kwargs):
		#realiza una consulta a la bdd y obtiene el nombre de las columnas de este objeto en la bdd


		_columnas = Core.getColumnas(self.object_migarte)
		columnas = ""

		valores = ""

		for columna_bd in _columnas:

			columna_nombre=columna_bd[0] #nombre columna
			tipo=columna_bd[1] #tipo columna
			default=str(columna_bd[2]).strip() #tiene un valor por defecto


			valor="%{}%".format(columna_nombre)

			#si el valor es un id y es un auto incremetable (tiene datos en el default)
			if columna_nombre == "id" and len(default) > 0:
				valor="default"
				continue


			
			

			if not columna_nombre in kwargs:

				#En caso de que no se definio f_creado en el modelo, pero existe en la tabla de la bdd
				#se asigna el valor now() ya que es probable que este campo se complete automaticamente
				#con la fecha actual
				if columna_nombre != "f_creado":

					print("No existe el atributo '{}' en la función format(). El valor es necesario para migrara a '{}'".format(columna_nombre,Core.getTabla(self.object_migarte)))
					exit()

				elif columna_nombre == "f_creado" and valor==None:
					valor="now()"
					continue


			
			
			if (tipo == "text" or tipo == "character varying" or tipo == "timestamp with time zone" or tipo == "varchar" or tipo=="date"):
				valor="\'%{}%\'".format(columna_nombre)

			#guarda el dato del objeto en el diccionario con el nombre de la columna
			if valores=="":
				valores=valor
			else:
				valores+=","+valor

			if columnas=="":
				columnas='"'+columna_bd[0]+'"'
			else:
				columnas+=',"'+columna_bd[0]+'"'


		
		return columnas,valores




class Exodo(object):

	objeto = None
	conn = None
	cursor=None
	encabezado=None

	table_name=None

	def __init__(self,objecto):
		self.objeto=objecto

		self.table_name = Core.getTabla(self.objeto)


	def make_insert(self,query,object_migarte):

		conexion = Conexion(self.objeto)
		self.conn = conexion.conn
		self.cursor = self.conn.cursor()
		self.cursor.execute(query)

		return Procesar(conn=self.conn,cursor=self.cursor,object_migarte=object_migarte)



	def insert(self,inserts=[]):

		largo_paginador = 70000

		conexion = Conexion(self.objeto)
		conn = conexion.conn
		cursor = conn.cursor()

		i=0
		while i<len(inserts):
			aux_insert =  (";".join(inserts[i:i+largo_paginador]))+";" 
			i=i+largo_paginador
			cursor.execute(aux_insert)
			conn.commit()
			#print(i)

		cursor.close()
		conn.close()





		






	



	












	










	

	

	


