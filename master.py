
from mifra.core.conexion import Conexion
from mifra.core.core import Core
from mifra.core.utility import Utility
from mifra.exodo import Exodo as E
from mifra.raw import Raw
import copy

class Master(object):

	raw = Raw
	Exodo = None

	def exodo(self):
		return E(self)

	def __init__(self):
		self.Exodo = E(self)
		 

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
				query += "\"{}\".\"{}\" IS NULL ".format(table_name,key, value)
			else:
				if str(type(value))=="<class 'str'>" :
					query += "\"{}\".\"{}\" = '{}'".format(table_name,key, value)
				else:
					query += "\"{}\".\"{}\" = {}".format(table_name,key, value)

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

	#Obtiene una instancia de una clase
	def first(self,**kwargs):
		table_name = Core.getTabla(self)

		top=""
		limit=""
		tipo_con = Conexion.getTipoConexion(self)
		if tipo_con == "sqlserver" : 
			top="TOP 1"

		query = "SELECT {top} * FROM {table_name}".format(top=top,table_name=table_name)

		for key, value in kwargs.items():

			if query == "SELECT {top} * FROM {table_name}".format(top=top,table_name=table_name):
				query += " WHERE "
			else:
				query += " AND "

			if value == None:
				query += "\"{}\".\"{}\" IS NULL ".format(table_name,key, value)
			else:
				if str(type(value))=="<class 'str'>" :
					query += "\"{}\".\"{}\" = '{}'".format(table_name,key, value)
				else:
					query += "\"{}\".\"{}\" = {}".format(table_name,key, value)

		query += " {limit} ;".format(limit=limit)


		con = Conexion(self)
		result=con.get(query)
		con.close()


		for res in result:

			aux= self.__class__()
			for index, atributo in enumerate(con.encabezado):	
				setattr(aux,atributo,res[index])

			return aux

		return None


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

				query=query+str(coma)+" \""+str(clave)+"\" = "+str(valor)
				coma=", "

			query=query+" where id = "+str(self.id)+";"

			#print(query)
			con = Conexion(self)
			con.execute(query)
			con.close()

		if  "id" in dir(self):
			self.find(self.id)


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

		#exit()

		con = Conexion(self)
		if "id" not in list(datos.keys()):
			con.execute(query)
			
		else:
			res=con.get(query)
			self.id=res[0][0]

		con.close()

		return self


	def get_insert(self,**kwargs):
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

		return query


	#crea un nuevo registro
	def make_sql_insert(self,**kwargs):




		table_name = Core.getTabla(self)


		raw=Raw().exec("""
		SELECT column_name,                 --Seleccionamos el nombre de columna
			data_type							--Seleccionamos el tipo de columna
			FROM information_schema.columns     --Desde information_schema.columns
			WHERE table_schema = 'public'       --En el esquema que tenemos las tablas en este caso public
			AND table_name   = '{table_name}' 
		""".format(table_name=table_name,)
		,con=getattr(self, 'Meta').db_conexion,con_close=False)

		result=raw.get()
		raw.commit()
		raw.con_close()
		
		columnas="("
		valores="("
		for columna in result:

			if columnas=="(":
				columnas+='"'+columna['column_name']+'"'
			else:
				columnas+=',"'+columna['column_name']+'"'


			if valores != "(":
				valores+=","

			if columna['column_name'] == "id":
				continue
			if columna['data_type']  == "text" or columna['data_type']  == "character varying" or columna['data_type']  == "timestamp with time zone" or columna['data_type']  == "varchar" or columna['data_type'] =="date":
				valores+="'{"+columna['column_name']+"}'"
			else:
				valores+="'{"+columna['column_name']+"}'"

		columnas+=")"
		valores+=")"

		return "INSERT INTO "+table_name+" "+columnas+" VALUES "+valores

		


	"""
		Elimina todos los registros y actualiza el auto incrementable
	"""
	def delete_and_restart_id(self,**kwargs):
		table_name = Core.getTabla(self)

		seq=table_name+'_id_seq'
		if 'seq' in kwargs:
			seq=kwargs['seq']

		query="""DELETE FROM {table_name}; ALTER SEQUENCE {seq} restart with 1;""".format(table_name=table_name,seq=seq)

		con = Conexion(self)
		con.execute(query)
		con.close()

	def delete_all(self,**kwargs):
		table_name = Core.getTabla(self)

		query="""DELETE FROM {table_name};;""".format(table_name=table_name)

		con = Conexion(self)
		con.execute(query)
		con.close()


	"""
		Imprime solo los atributos que pertenecen al objeto
	"""
	def __str__(self):
		try:
			getattr(self, 'Meta').db_table
		except Exception as e:
			#para los casos que no tiene Meta.db_table
			clase="Class "+str(str(type(self))[str(type(self)).rfind('.')+1:-2]).upper()+" :\n\n"
	
			for columna in self.__dict__.items():
				
				if columna[0]=='Meta' or columna[0]=='Exodo' :
					continue
				clase+=columna[0]+" :"+str(columna[1])+"\n"

			return clase

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

	def make_update(self,**kwargs):

		if not 'fields' in kwargs:
			raise ValueError("Falta el campo 'fields' en  make_update(fields=['campo_1','campo_2']) ")

		table_name = Core.getTabla(self)

		attrs=[]
		for attr in kwargs['fields']:
			if str(type(attr)) == "<class 'tuple'>":
				attrs.append(attr[0])
			else:
				attrs.append(attr)

	
	

		raw=Raw().exec("""
		SELECT column_name,                 --Seleccionamos el nombre de columna
			data_type							--Seleccionamos el tipo de columna
			FROM information_schema.columns     --Desde information_schema.columns
			WHERE table_schema = 'public'       --En el esquema que tenemos las tablas en este caso public
			AND table_name   = '{table_name}' 
			and column_name in ({column_name})
		""".format(table_name=table_name,column_name=str(attrs)[1:-1])
		,con=getattr(self, 'Meta').db_conexion,con_close=False)

		result=raw.get()
		raw.commit()
		raw.con_close()
		
		columnas={}
		for columna in result:
			if columna['data_type']  == "text" or columna['data_type']  == "character varying" or columna['data_type']  == "timestamp with time zone" or columna['data_type']  == "varchar" or columna['data_type'] =="date":
				columnas[columna['column_name']]=True
			else:
				columnas[columna['column_name']]=False

		i=-1
		valores=""
		for field in kwargs['fields']:

			conf={
					'quotation_mark':True #Determina si se agregan las comillas dependiendo del tipo de dato
				  }


			attr=field
			if str(type(field)) == "<class 'tuple'>":
				attr=field[0]
				if len(field)>1 and 'quotation_mark' in field[1]:
					conf['quotation_mark']=field[1]['quotation_mark']


			i+=1
			#valor = getattr(self, attr )
			valor = "{"+attr+"}"

			if not attr in columnas:
				raise ValueError("No existe '"+attr+"' en la tabla "+table_name)

			if columnas[attr] and valor != None and conf['quotation_mark']==True:
				valor="'"+valor+"'"

			valores+="\""+attr+"\""+" = "+str(valor)+" ,"
			
		
		valores=valores[0:-1]

		if not 'where' in kwargs  or ('where' in kwargs and kwargs['where']==True):
			return " UPDATE \"cr_potenciales_renovantes\" SET "+valores+" WHERE {where};"

		return " UPDATE \"cr_potenciales_renovantes\" SET "+valores+"  ;"



	def make_dict(self,**kwargs):

		if 'query' in kwargs:
			registros = self.select(kwargs['query'])
		else:
			registros = self.all()

		dic={}
		for registro in registros:
			dic[getattr(registro, kwargs['key']).strip()]=registro

		return dic

	"""
		Comprueba si un objeto tiene los mismos valores en sus atributos
		retorna:
			result['equals'] == True si es igual False si un atributo es distinto
			result['diff'] Retorna una lista de los atributos que no son iguales
			result['detail']['atributo'] Entrega detalle del atributo ej: {'values': {'self': '13-03-2023', 'other': '13-03-2023'}, 'equals': True}
				... donde 'values' muestra los valores comprobados
				    y 'equals' si son iguales

	"""
	def equals(self,other,**kwargs):
		check=kwargs['check']
		to_string=[]
		def_equals={}
		if 'to_string' in kwargs:
			to_string=kwargs['to_string']

		if 'def_equals' in kwargs:
			def_equals=kwargs['def_equals']
		

		result={}
		result['equals']=True
		result['diff']=[] #almacena los campos diferentes
		result['detail']={}

		for attr in check:

			try:
				self_value=getattr(self, attr )
			except Exception as e:
				print("No existe "+attr+" en "+str(str(type(self))[str(type(self)).rfind('.')+1:-2]).upper())
				exit()
			try:
				object_value=getattr(other, attr )
			except Exception as e:
				print("No existe "+attr+" en "+str(str(type(other))[str(type(other)).rfind('.')+1:-2]).upper())
				exit()


			result['detail'][attr]={}
			



			if attr in to_string:
				self_value=str(self_value)
				object_value=str(object_value)


			equals=False

			if self_value == object_value:
				equals=True

			if attr in def_equals:
				function = def_equals[attr]
				equals = function(getattr(self, attr ),getattr(other, attr ))
				if str(type(equals)) == "<class 'tuple'>":
					self_value=equals[1]
					object_value=equals[2]
					equals=equals[0]



			result['detail'][attr]['values']={'self':self_value,'other':object_value}
			if equals:
				result['detail'][attr]['equals']=True
			else:
				result['equals']=False
				result['detail'][attr]['equals']=False
				result['diff'].append(attr)


		return result


	def changue_db_table(self,db_table):
		
		self.Meta.db_table=db_table


	



