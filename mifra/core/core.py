from mifra.core.conexion import Conexion

class Core(object):



	#comprueba si existe la clase Meta
	def existMeta(object):

		try:
			attr = getattr(object, 'Meta')
		except:
			print("ERROR: No existe la clase Meta en {}".format(type(object)))
			exit()


	#Retorna el nombre de la tabla asignado en Meta.db_table
	def getTabla(object):
		#comprueba si existe la clase Meta
		Core.existMeta(object)

		try:
			return getattr(object, 'Meta').db_table
		except:
			print("ERROR: No existe el campo 'db_table' en la clase Meta() en {}".format(type(object)))
			exit()



	#obtiene las columnas de la tabla
	def getColumnas(object):

		tipo_con = Conexion.getTipoConexion(object)

		sql="""
			SELECT column_name,                 --Seleccionamos el nombre de columna
			data_type,							--Seleccionamos el tipo de columna
			column_default						--Tiene valor por defecto
			FROM information_schema.columns     --Desde information_schema.columns
			WHERE table_schema = 'public'       --En el esquema que tenemos las tablas en este caso public
			AND table_name   = '{}';
		""".format(Core.getTabla(object))

		if tipo_con == "sqlserver":

			sql = """
				SELECT COLUMN_NAME, 			--Seleccionamos el nombre de columna
				DATA_TYPE, 						--Seleccionamos el tipo de columna
				COLUMN_DEFAULT  				--Tiene valor por defecto
				FROM information_schema.columns --Desde information_schema.columns
				WHERE table_name = '{}'
				order by ordinal_position
			""".format(Core.getTabla(object))

		

	
		con = Conexion(object)
		lista = con.get(sql)
		con.close()

		if len(lista) == 0:
			print("ERROR: No existe la tabla '{}' señalada en clase '{}'".format(Core.getTabla(object),type(object)))
			exit()

	
		return lista




	#recorre los atributos y valida que existan el la clase
	#retorna las columnas del objeto (Excepto el ID)
	def validarAtributos(object,**kwargs):
		#realiza una consulta a la bdd y obtiene el nombre de las columnas de este objeto en la bdd

		if not 'columnas' in kwargs:
			columnas = Core.getColumnas(object)
		else:
			columnas = kwargs["columnas"]

		attr = {}
		for columna_bd in columnas:

			columna_nombre=columna_bd[0] #nombre columna
			tipo=columna_bd[1] #tipo columna
			default=str(columna_bd[2]).strip() #tiene un valor por defecto

			#si el valor es un id y es un auto incremetable (tiene datos en el default)
			if columna_nombre == "id" and len(default) > 0:
				attr["id"]="default"
				continue


			valor = None
			try:
				#getattr : Obtiene el valor del atributo de la clase
				valor =getattr(object, columna_nombre )

			except Exception as e:

				#En caso de que no se definio f_creado en el modelo, pero existe en la tabla de la bdd
				#se asigna el valor now() ya que es probable que este campo se complete automaticamente
				#con la fecha actual
				if columna_nombre != "f_creado":

					print("No existe el atributo {} en {}".format(columna_nombre,type(object)))
					exit()

				elif columna_nombre == "f_creado" and valor==None:
					attr["f_creado"]="now()"
					continue


			

			if valor!=None and (tipo == "text" or tipo == "character varying" or tipo == "timestamp with time zone" or tipo == "varchar" or tipo=="date"):

				if tipo=="date":
					_fecha=valor
					valor=str(valor).replace("/","-").replace(" ","").replace("'","")
					if len(valor) != 10 or not "-" in valor:
						print("ERROR: El formato de fecha debe ser dd-mm-yyyy o yyyy-mm-dd en {}".format(_fecha))
						exit()

					valor=valor.split("-")
					if len(valor[0]) == 4:
						valor=valor[0]+"-"+valor[1]+"-"+valor[2]
					elif len(valor[2]) == 4:
						valor=valor[2]+"-"+valor[1]+"-"+valor[0]
					else:
						print("ERROR: El formato de fecha debe ser dd-mm-yyyy o yyyy-mm-dd en {}.".format(_fecha))
						exit()


				#reemplazo lo ' por '' para no tener problema al insertar
				if valor != None:
					valor="\'{}\'".format(str(valor).replace("'","''"))

			#guarda el dato del objeto en el diccionario con el nombre de la columna
			attr[columna_nombre]=valor

		
		return attr
	


	#retorna los atributos de una clase
	#(Excepto el ID)
	def getAttr(object):
		return Core.validarAtributos(object)


