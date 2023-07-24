import settings
from mifra.core.conexion import Conexion
from mifra.core.core import Core
import numpy as np
import pandas as pd
import psycopg2

class QuerySet(object):

	insert    =""
	table_name=""
	columns   =""
	values    =""
	_object   =None

	def __init__(self,insert,**kwargs):
		self.insert=insert
		self.table_name=kwargs['table_name']
		self.columns=eval("("+kwargs['columns']+")")
		self.values=eval("("+kwargs['values']..replace('\r\n', '').replace('\r', '').replace("\n","").replace("true","True")+")")
		self._object=kwargs['object']


	def set(attr,value,**kwargs):
		pass


	def get_object(self):

		aux = self._object
		for index, atributo in enumerate(self.columns):	
			setattr(aux,atributo,self.values[index])

		return aux

	def set_object(self,_object):
		values=()
		for column in self.columns:
			values=values+(getattr(_object, column ),)
		self.values=values
		self._object=_object

		self.get()

	def set_table_name(self,table_name):
		self.table_name=table_name

	def get(self):
		self.insert="INSERT INTO {table_name} {columnas} VALUES {values}".format(table_name=self.table_name,columnas=str(self.columns).replace("'","\""),values=str(self.values))
		self.insert=self.insert.replace("'None'","NULL").replace("None","NULL")
		return self.insert


	def __str__(self):
		return self.get()


class Procesar(object):

	data = []
	cursor=None
	conn = None
	object_migarte=None
	exec_type = None
	df=None

	def __init__(self,**kwargs):

		self.conn = kwargs['conn']

		if 'df' in kwargs:
			self.df=kwargs['df']



		self.cursor = kwargs['cursor']
		self.object_migarte = kwargs['object_migarte']
		self.exec_type = kwargs['exec_type']


	def get_data_function(dato,encabezado,row):
		params={}

		def get_value(index):
			if index in encabezado:
				return row[encabezado[index]]

		params['header']=encabezado
		params['data']=dato
		params['row']=row
		params['get_value']=get_value

		return params



	"""
		Obtiene los id de las dependencias
		la idea es generar un diccionario con el campo clave que contiene el id
	"""
	def getDependencias(object_migarte,**kwargs):

		

		con_params = Conexion.get_db_conexion(object_migarte)

		aux_conn = Conexion.getCon(con_params)



		aux_cursor = aux_conn.cursor()
		
		dependencias = {}
		for kwarg in kwargs:
			#busca los kwargs que sean una tupla
			if str(type(kwargs[kwarg])) == "<class 'tuple'>":


				#si el primer valor de la tupla del diccinario es una función...entonces no es una dependencia
				if str(type(kwargs[kwarg][0])) == "<class 'function'>":
					continue

				dependencias[ kwargs[kwarg][1] ]={}

		
				query = ""
				
				if  str(kwargs[kwarg][0]).replace(" ","")[:6].upper() == "SELECT":
					query = str(kwargs[kwarg][0])

					#Aquí solo validamos que no haya ingresado id
					#Si no lo ha ingresado se agrega a la query
					tabla = str(query)[str(query).upper().find(" FROM "):].replace("'","").replace('"',"").replace("from","").replace("FROM","").strip()[:str(query)[str(query).upper().find(" FROM "):].replace("'","").replace('"',"").replace("from","").replace("FROM","").strip().find(" ")]
					
					_from = query[query.upper().rfind("FROM")+4:].strip().replace("  "," ")
			
					tabla = _from.split(" ")[0]
					_tabla =_from.replace(tabla,"").strip().split(" ")[0].upper() 

			
					if len(_tabla)>0 and _tabla != "ORDER" and _tabla != "GROUP" and _tabla != "WHERE" and _tabla != "INNER":
						tabla =_tabla



				
					if not "id" in str(query)[:str(query).upper().find(" FROM ")].replace("select","").replace("SELECT","").replace(" ","").replace("'","").replace('"',"").replace(tabla+".","").split(","):
						query=query.replace("select","select "+tabla+".id,").replace("SELECT","SELECT "+tabla+".id,")

					
				else:

					campo = str(kwargs[kwarg][0]).split(".")
					if len(campo) == 1:
						print( "Para invocar una dependencia es necesario este formato: campo = ('columna_query','tabla.atributo',)," )
						exit()

					query = "select id,{campo} from {tabla}".format(campo=campo[1],tabla=campo[0])

				aux_cursor.execute(query)
				for rows in aux_cursor.fetchall():
					key=""
					i=0
					for row in rows:
						i+=1
						if i == 1:
							continue
						if row == None:
							row=""
						if key != "":
							key+="+"
						key+=str(row)
					dependencias[ kwargs[kwarg][1] ][ str(key) ]=str(rows[0])

		aux_cursor.close()
		aux_conn.close()
		"""
			el campo del objeto base mas el codigo y su id del objeto referencia
			{'CODCLI': {'20091100029': 1, 'I000118664793': 2},'CODCARR': {'101C': 1, '101P': 2}
		"""
		return dependencias


	def get_config(tupl):

		conf={}
		conf['valid_exist']=True
		conf['print_not_exist']=False
		conf['restructure']=None
		conf['exclude_if_not_exist']=False

		#Validaciones
		if len(tupl) < 3:
			return conf

		if 'valid_exist' in tupl[2]:
			conf['valid_exist']=tupl[2]['valid_exist']

		if 'print_not_exist' in tupl[2]:
			conf['print_not_exist']=tupl[2]['print_not_exist']

		if 'restructure' in tupl[2]:
			conf['restructure']=tupl[2]['restructure']

		if 'exclude_if_not_exist' in tupl[2]:
			conf['exclude_if_not_exist']=tupl[2]['exclude_if_not_exist']

		return conf



	def format(self,**kwargs):
		
		dependencias = Procesar.getDependencias(self.object_migarte,**kwargs)
		columnas,_valores = self.validarAtributos(**kwargs)
		table_name =  Core.getTabla(self.object_migarte)

		datos=[]
		if self.exec_type == "_df":
			i=0
			encabezado={}
			for col in self.df.columns:
				encabezado[col]=i
				i+=1

			datos=[]
			df = self.df.where(pd.notnull(self.df), None)
			datos = df.values
			#print(df.values)
			#for i in range(len(df)):
			#	datos.append(df.iloc[i, :].tolist())

			#print(datos)
			#exit()


		if self.exec_type == "_query":

			i=0
			encabezado={}
			for col in self.cursor.description:
				encabezado[col[0]]=i
				i+=1

			datos = self.cursor.fetchall()
			self.cursor.close()
			self.conn.close()


		inserts=[]
		valid_errors=[]
		exit_app=False
		for row in datos:
			valores=_valores

			exclude=False #Determina si se exclude esta fila

			for attr in kwargs:


				"""
					Las tuplas en el diccionario pueden traer una función o una dependencia
				"""
				if str(type(kwargs[attr])) == "<class 'tuple'>":

					#Configuraciones
					conf = Procesar.get_config(kwargs[attr])
				

					if len(kwargs[attr])>=2 and str(type(kwargs[attr][0])) == "<class 'str'>" and str(type(kwargs[attr][1])) == "<class 'str'>":
						"""
							Para los casos de dependencias
						"""

						columna = kwargs[attr][1]

						if "+" in columna:
								_columnas = columna.replace(" ","").split('+')
								valor_columna=""
								for aux_culumna in _columnas:
									if valor_columna != "":
										valor_columna+="+"

									if not aux_culumna in encabezado:
										print("No existe "+aux_culumna+" en la query de origen")
										exit()

									if row[encabezado[aux_culumna]] == None:
										valor_columna+=""
									else:
										valor_columna+=str(row[encabezado[aux_culumna ]])
									
						else:

							if not columna in encabezado:
								print("No existe "+columna+" en la query de origen")
								exit()

							valor_columna = str(row[encabezado[columna]])
							#recibe una funcion para reestructurar el dato
							if conf['restructure']!=None:
								data=Procesar.get_data_function(row[encabezado[columna]],encabezado,row)
								restructure = conf['restructure']
								valor_columna=restructure(data)


						try:

							valores=valores.replace("%"+attr+"%",   str(dependencias[  columna  ][ valor_columna] )    )

						except Exception as e:
							valores=valores.replace("%"+attr+"%",   str("None")    )
							
							if conf['exclude_if_not_exist'] == True:
								exclude=True
							
							elif conf['valid_exist'] == True or  conf['print_not_exist']==True:
								error = "["+columna+"] No existe "+str(valor_columna)+" ("+columna+") en '"+kwargs[attr][0]+"'." 
								if conf['valid_exist'] == True:
									error += " Si desea omitir esta validación y dejar NULL los id que no existen, agregue {'valid_exist':False} " 
								if not error in valid_errors:
									valid_errors.append(error)
								if conf['valid_exist'] == True:
									exit_app=True
							

					else:

						"""
							Para los casos de funciones
						"""

						if str(type(kwargs[attr][0])) != "<class 'function'>":
							print("Primero debe ir la función y luego el nombre del campo en format() "+str(kwargs[attr]))
							exit()

						dato=None	

						if len(kwargs[attr])>1 and not kwargs[attr][1] in encabezado:
							print("No existe "+str(kwargs[attr][1])+" en la query de origen")
							exit()
						elif len(kwargs[attr])>1:
							dato = row[encabezado[kwargs[attr][1]]]


						function = kwargs[attr][0]
						params={}

						def get_value(index):
							if index in encabezado:
								return row[encabezado[index]]

						#params['header']=encabezado
						#params['data']=dato
						#params['row']=row
						#params['get_value']=get_value
						data=Procesar.get_data_function(dato,encabezado,row)
						valor = function(data)
						

						valores=valores.replace("%"+attr+"%", str(valor).replace("'","`") )

	

				else:

					if kwargs[attr] and not kwargs[attr] in encabezado:
							print("No existe '"+str(kwargs[attr])+"' en la query de origen")
							exit()

					if not kwargs[attr] or len(str(row[encabezado[kwargs[attr]]]).replace(" ","")) == 0 :
						valores=valores.replace("%"+attr+"%", str('None') )
					else: 
						valores=valores.replace("%"+attr+"%", str(row[encabezado[kwargs[attr]]]).replace("'","`") )


			

			#insert = """INSERT INTO {table_name} ({columnas}) VALUES ({valores}) ||;||""".format(table_name=table_name,columnas=columnas,valores=valores)
			insert = """INSERT INTO {table_name} ({columnas}) VALUES ({valores});""".format(table_name=table_name,columnas=columnas,valores=valores)


			
			insert=QuerySet(insert.replace("'None'","NULL").replace("None","NULL").replace("True","true"),
																					table_name=table_name,
																					columns=columnas,
																					values=valores,
																					object=self.object_migarte,
																					)
			if not exclude:
				inserts.append(insert)
				#inserts += " "+insert.replace("'None'","NULL").replace("None","NULL").replace("True","true")


		if valid_errors:
			for error in valid_errors:
				print("* "+error)
			if exit_app:
				exit()


		

		#inserts=inserts[:inserts.rfind("||;||")]

		#return inserts.split("||;||")
		return inserts


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

					print("No existe el atributo '{}' en la función format(). El valor es necesario para migrar a '{}'".format(columna_nombre,Core.getTabla(self.object_migarte)))
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

	def get_table_name(self):
		if not self.table_name:
			self.table_name = Core.getTabla(self.objeto)
		return self.table_name


	def __init__(self,objecto):
		
		self.objeto=objecto

		


	def make_insert(self,query,object_migarte):

		conexion = Conexion(self.objeto)
		self.conn = conexion.conn
		self.cursor = self.conn.cursor()
		self.cursor.execute(query)

		return Procesar(conn=self.conn,cursor=self.cursor,object_migarte=object_migarte,exec_type='_query')

	def df_make_insert(self,df):
		conexion = Conexion(self.objeto)
		self.conn = conexion.conn
		self.cursor = self.conn.cursor()
		self.cursor.execute("select * from "+Core.getTabla(self.objeto)+" limit 1")

		return Procesar(conn=self.conn,cursor=self.cursor,object_migarte=self.objeto,df=df,exec_type='_df')



	def insert(self,q_inserts=[],**kwargs):

		#Es la cantidad de registros que se insertan por iteracion
		largo_paginador = 70000
		if 'paginator' in kwargs:
			largo_paginador = kwargs['paginator']

		#Es la cantidad de iteraciones
		iterator=None
		if 'iterator' in kwargs:
			iterator = kwargs['iterator']


		conexion = Conexion(self.objeto)
		conn = conexion.conn
		cursor = conn.cursor()

		i=0
		iteraciones=0

		inserts=[]
		for insert in q_inserts:
			inserts.append(insert.get())



		while i<len(inserts):
			iteraciones+=1
			aux_insert =  (";".join(inserts[i:i+largo_paginador]))+";" 
			i=i+largo_paginador
			cursor.execute(aux_insert)
			conn.commit()

			if iterator and iterator == iteraciones:
				break

		inserts=None

		cursor.close()
		conn.close()


	def insert_or_update(self,q_inserts=[],**kwargs):
		if not q_inserts or len(q_inserts)==0:
			return


		table_name=self.get_table_name()
		new_table_name = "aatemp_"+table_name

		inserts=[]
		for insert in q_inserts:
			insert.set_table_name(new_table_name)
			inserts.append(insert.get())



		#print(inserts[0])

		if not 'match' in kwargs:
			print("""Es necesario agregar un parametro 'match' para determinar cual es el campo de union para el update EJ: 
        		PotencialMatriculable().Exodo.insert_or_update(inserts,match=['codigo_matricula'],update_only=['monto_deuda','bloqueo_biblioteca'])
																								""")
			exit()

		update_only="*"
		if 'update_only' in kwargs:
			update_only=kwargs['update_only']



		

		#table_name=str((str(inserts[0])[str(inserts[0]).strip().find("INSERT INTO")+len("INSERT INTO")+1:])[:(str(inserts[0])[str(inserts[0]).strip().find("INSERT INTO")+len("INSERT INTO")+1:]).strip().find('("')+len('("')-1]).strip()
		
		#inserts = [w.replace('INSERT INTO {}'.format(table_name), 'INSERT INTO {}'.format(new_table_name)) for w in inserts]



		largo_paginador = 70000

		conexion = Conexion(self.objeto)
		conn = conexion.conn
		cursor = conn.cursor()

		#validamos si la tabla existe...de ser asi la eliminamos
		query="SELECT table_name FROM information_schema.columns WHERE table_name='{new_table_name}'".format(new_table_name=new_table_name)
		cursor.execute(query)
		existe = cursor.fetchall()
		if len(existe)>0:
			query = "drop table {new_table_name} ;".format(new_table_name=new_table_name)
			cursor.execute(query)
			conn.commit()
	

		
		new_table = "create table {new_table_name}  as select * from {table_name} limit 1;".format(new_table_name=new_table_name,table_name=table_name)
		new_table += "delete from {new_table_name};".format(new_table_name=new_table_name)
		cursor.execute(new_table)
		conn.commit()

		i=0
		while i<len(inserts):
			aux_insert =  (";".join(inserts[i:i+largo_paginador]))+";" 
			i=i+largo_paginador
			cursor.execute(aux_insert)
			conn.commit()
			#print(i)
	
		

		equals=kwargs['match']
		"""
		********************************************************************************************
											Validación condiciones
		"""
		where_update = ""
		where_insert = ""
		for value in equals:
			###################################################################
			#######################   PARA EL UPDATE   ########################
			if where_update != "":
				where_update += " AND "

			where_update += " (\"{table_name}\".\"{value}\" = \"{new_table_name}\".\"{value}\" AND \"{new_table_name}\".\"{value}\" is not NULL) ".format(table_name=table_name, value=value , new_table_name=new_table_name)
			###################################################################
			#######################   PARA EL INSERT   ########################
			#if where_insert != "":
			#	where_insert += "concat("
			#where_insert += "\"{value}\" not in (select \"{table_name}\".\"{value}\" from \"{table_name}\" where \"{table_name}\".\"{value}\" is not null)".format(table_name=table_name, value=value)
			where_insert += "\"{table}\"."+value+",'-',"

		where_insert="concat("+(where_insert[:-1].format(table=new_table_name))+") not in (select concat("+(where_insert[:-1].format(table=table_name))+") from "+table_name+")"
		"""
		********************************************************************************************
		"""
		"""
		********************************************************************************************
											Validación columnas
		"""

		columnas=""
		#aux_columnas = str(inserts[0][inserts[0].find('(')+1:inserts[0].find(')')]).replace('","','"|-|-|"').split("|-|-|")
		aux_columnas = q_inserts[0].columns
		columnas_insert=""

	
		

		if update_only != "*":
			for aux in update_only:
				if  not ''+aux+'' in aux_columnas:
					print(aux_columnas)
					print("No existe la columna '"+aux+"' en la tabla "+table_name)
					exit()

		

		for columna in aux_columnas:

			###################################################################
			#######################   PARA EL INSERT   ########################
			if columnas_insert == "":
				columnas_insert = ''
			else:
				columnas_insert += ', '
			columnas_insert += '"'+columna+'"'
			
			###################################################################
			#######################   PARA EL UPDATE   ########################

			if update_only != "*":
				if columna.replace('"',"") not in update_only:
					continue

			if columnas == "":
				columnas = ' SET '
			else:
				columnas += ', '
			columnas += '"'+columna+'" = "'+ new_table_name+'"."'+columna+'"'
			
		"""
		********************************************************************************************
		"""

		update="""
		update {table_name} {columnas}
		from {new_table_name}
		where {where_update};
		""".format(columnas=columnas,table_name=table_name,new_table_name=new_table_name,where_update=where_update)

		if 'print_update' in kwargs and kwargs['print_update']==True:
			print(update)



		#print(update)
		insert="""
		insert into {table_name} ({columnas_insert})
		select {columnas_insert}
		from {new_table_name}
		where {where_insert};

		""".format(columnas_insert=columnas_insert,table_name=table_name,new_table_name=new_table_name,where_insert=where_insert)



		drop="""
		drop table {new_table_name} ;
		""".format(new_table_name=new_table_name)

		#drop=""

		#print(update)
		#print(insert)
		cursor.execute(update+" "+insert+drop)
		conn.commit()




		inserts=None

		cursor.close()
		conn.close()
	

	def df_create_tabla(df,**kwargs):



		if not 'table_name' in kwargs:
			print("Falta el parametro 'table_name' en la función df_create_tabla() ")
			exit()

		if not 'db_conexion' in kwargs:
			print("Falta el parametro 'db_conexion' en la función df_create_tabla() ")
			exit()

		table_name = kwargs['table_name']

		from mifra.master import Master
		class Estandar(Master):

			def __init__(self):

				pass


			class Meta:
				#aquí el nombre de la tabla
				db_table = table_name
				db_conexion = kwargs['db_conexion']


		

		measurer = np.vectorize(len)

		#obtiene las columnas
		res1 = measurer(df.values.astype(str)).max(axis=0)


		columnas=""
		for columna in df.columns.values:
			columnas+="\""+columna.strip().replace('"',"'")+"\","
		columnas=columnas[:-1]



		create = "CREATE TABLE "+table_name+" ("
		commillas=[]
		i=-1
		for columna in df.columns.values:
			i+=1

			tipo="text"
			if str(df[columna].dtypes) == "object":
				tipo="varchar("+str(res1[i])+")"
				commillas.append(True)

			if 'int' in str(df[columna].dtypes):
				tipo="bigint"
				commillas.append(False)

			if 'float' in  str(df[columna].dtypes) :
				tipo="real"
				commillas.append(False)

			create += " \""+columna+"\" "+tipo+" NULL,"

			

		inserts=[]
		for i in range(len(df)): 

			insert="INSERT INTO "+table_name
			insert+=" ("+columnas+") "
			

			num_columna=-1
			value=""
			for columna_name in df.columns.values:
				num_columna+=1
				#print(str(df.loc[i,columna_name]))

				valor=str(df.loc[i,columna_name])
				if valor == 'nan' :
					valor = 'NULL'

				if commillas[num_columna] == True and valor != 'NULL':
					value+="\'"+str(valor).replace("'","''")+"\',"
				else:
					value+=""+str(valor).replace("'","''")+","

			value=value[:-1]
			insert+="VALUES ( "+value+" ); "
			inserts.append(insert)



		create += ");"
		create = create.replace(",);",");")

		#Seteamos el nombre de la tabla
		objeto=Estandar()
		#meta = getattr(objeto, 'Meta')
		#setattr(meta, 'db_table', table_name)
		#setattr(objeto, 'Meta', meta)

		#print(getattr(objeto, 'Meta').db_table)

		conexion = Conexion(objeto)
		conn = conexion.conn
		cursor = conn.cursor()

		query="SELECT table_name FROM information_schema.columns WHERE table_name='{table_name}'".format(table_name=table_name)
		cursor.execute(query)
		existe = cursor.fetchall()
		if len(existe)>0:
			query = "drop table {table_name} ;".format(table_name=table_name)
			cursor.execute(query)
			conn.commit()

		cursor.execute(create)
		conn.commit()
		cursor.close()
		conn.close()

		objeto.exodo().insert(inserts)
