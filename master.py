import settings
from mifra.core.conexion import Conexion
from mifra.core.core import Core
from mifra.core.utility import Utility
from mifra.exodo import Exodo
from mifra.raw import Raw

class Master(object):

	raw = Raw

	def exodo(self):
		return Exodo(self)

	def __init__(self):
		pass 

	"""
		recibe una query y retorna un objecto con los atributos de la tabla de la query
	"""
	def select(self,query):

		con = Conexion(self)
		result=con.get(query)
		con.close()

		lista=[]

		for res in result:

			aux= self.__class__()

			for index, atributo in enumerate(con.encabezado):	
				setattr(aux,atributo,res[index])

			lista.append(aux)

		

		return lista



	"""
		Retorna una lista de objetos
	"""
	def all(self):
		return self.filter()

	#retorna una lista de objetos
	#recibe un diccionario de argumentos
	#ej: detalle.filter(ad_solicitud_id=self.id , rut = '12345678')
	def filter(self,**kwargs):
		table_name = Core.getTabla(self)
		query = "SELECT * FROM {}".format(table_name)

		for key, value in kwargs.items():
			


			if query == "SELECT * FROM {}".format(table_name):
				query += " WHERE "
			else:
				query += " AND "

			if value == None:
				query += "{} IS NULL ".format(key, value)
			else:
				if str(type(value))=="<class 'str'>" :
					query += "{} = '{}'".format(key, value)
				else:
					query += "{} = {}".format(key, value)

		query += ";"

		

		con = Conexion(self)
		result=con.get(query)
		con.close()

		lista=[]

		for res in result:

			aux= self.__class__()
			for index, atributo in enumerate(con.encabezado):	
				setattr(aux,atributo,res[index])

			lista.append(aux)

		return lista

	#retorna una lista de objetos
	#recibe un string en
	#EJ: detalle.where("ad_solicitud_id = {}".format(self.id))
	def where(self,where):

		table_name = Core.getTabla(self)

		query="SELECT * FROM  public.{table}".format(table=table_name)

		if len(where)>0:
			query = str(query) + " where " + str(where).replace("where","")

		#print(query)

		con = Conexion(self)
		result=con.get(query)
		con.close()

		lista=[]

		for res in result:

			aux= self.__class__()
			for index, atributo in enumerate(con.encabezado):	
				setattr(aux,atributo,res[index])
			lista.append(aux)

		return lista


	#Obtiene una instancia de una clase
	def find(self,id):

		table_name = Core.getTabla(self)
		con = Conexion(self)

		query="SELECT * FROM  public.{table} where id = {id} LIMIT 1".format(table=table_name,id=id)
		res=con.get(query)
		con.close()

		if len(res)==0:
			self.id=0
			return self

		res=res[0]


		for index, atributo in enumerate(con.encabezado):			
			setattr(self,atributo,res[index])

		return self


	def save(self):

		if not "id" in dir(self) or "id" in dir(self) and self.id == None:
			self.new()

		else:

			columnas = Core.getAttr(self)#retorn un diccionario
			table_name = Core.getTabla(self)

			query="UPDATE public.{table} SET ".format(table=table_name)

			coma=""
			for clave, valor in columnas.items():
				if valor == None:
					valor ="NULL"
				if clave == "id":
					continue

				query=query+str(coma)+str(clave)+" = "+str(valor)
				coma=", "

			query=query+" where id = "+str(self.id)+";"

			#print(query)
			con = Conexion(self)
			con.execute(query)
			con.close()

		if  "id" in dir(self):
			self.find(self.id)





	#crea un nuevo registro
	def new(self):

		#retorna los atributos de una clase
		#excepto el id
		datos = Core.getAttr(self)
		table_name = Core.getTabla(self)

		valores= None
		columnas = None


		valores,columnas =Utility.getValueAttr(datos)

		
		query="INSERT INTO public.{table} ({columnas}) VALUES ({valores})".format(table=table_name,columnas=columnas,valores=valores)


		if "id" in list(datos.keys()):
			query=query+" returning id"

		query=query+";"

		#print(query)
		#exit()

		con = Conexion(self)
		if "id" not in list(datos.keys()):
			con.execute(query)
			
		else:
			res=con.get(query)
			self.id=res[0][0]

		con.close()

		return self


	"""
		Elimina todos los registros y actualiza el auto incrementable
	"""
	def delete_and_restart_id(self):
		table_name = Core.getTabla(self)

		query="""DELETE FROM {table}; ALTER SEQUENCE {table}_id_seq restart with 1;""".format(table=table_name)

		con = Conexion(self)
		con.execute(query)
		con.close()


	"""
		Imprime solo los atributos que pertenecen al objeto
	"""
	def __str__(self):
		clase=str(str(type(self))[str(type(self)).rfind('.')+1:-2]).upper()+" :\n"
	
		for columna in Core.getColumnas(self):
			try:
				valor = str(getattr(self, columna[0] ))
				if len(valor) > 100:
					valor = valor[:50]+"   ...   "+valor[-50:]
				clase+="{} : {}\n".format(columna[0],valor)
			except:
				clase+="{} : {}\n".format(columna[0],"")
		

		return clase

	"""
		Imprime todos los atributos del objeto e incluso las funciones
	"""
	def print(self):

		clase=str(str(type(self))[str(type(self)).rfind('.')+1:-2]).upper()+" :\n"

		funciones=""
		for columna in dir(self):


			if columna=='Meta' or columna.strip()[0:2] == "__" or len(columna.strip())>=2 and columna.strip()[-2:]=="__":
				continue


			if columna in ['select',
							'all',
							'filter',
							'where',
							'find',
							'save',
							'new',
							'print',]:

				funciones+="{} : {}\n".format(columna+"()",Utility.descFuncion(columna))

			else:
				try:
					valor = str(getattr(self, columna ))
					if len(valor) > 100:
						valor = valor[:50]+"   ...   "+valor[-50:]
					clase+="{} : {}\n".format(columna,valor)
				except:
					clase+="{} : {}\n".format(columna,"")


		print(clase+"\n"+funciones)











	

	

	


