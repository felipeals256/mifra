from mifra.raw import Raw
def get_table(query,**kwargs):
	#Aquí solo validamos que no haya ingresado id
	#Si no lo ha ingresado se agrega a la query
	tabla = str(query)[str(query).upper().find(" FROM "):].replace("'","").replace('"',"").replace("from","").replace("FROM","").strip()[:str(query)[str(query).upper().find(" FROM "):].replace("'","").replace('"',"").replace("from","").replace("FROM","").strip().find(" ")]
	
	_from = query[query.upper().rfind("FROM")+4:].strip().replace("  "," ")

	tabla = _from.split(" ")[0]
	_tabla =_from.replace(tabla,"").strip().split(" ")[0].upper() 


	if len(_tabla)>0 and _tabla != "ORDER" and _tabla != "GROUP" and _tabla != "WHERE":
		tabla =_tabla
		
	return tabla


def get_dictionary(query,con,**kwargs):

	tabla=get_table(query)


	query=query.replace("select","select "+tabla+".id as _id_dependencia,").replace("SELECT","SELECT "+tabla+".id,")

	result = Raw.query(query,con=con)
	dictionary={}
	for res in result:
		key="+"
		for index, valor in res.items():
			if index == '_id_dependencia':
				continue
			if key == "+":
				key=str(valor)
			else:
				key+="+"+str(valor)
		dictionary[key]=res['_id_dependencia']

	return dictionary

class Utility(object):

	def __init__(self):
		pass 

	#es un método recursivo
	def getValueAttr(datos,i=0, valores="", columnas = ""):
	
		if i < len(datos):

			columna = list(datos.keys())
			valor = list(datos.values())

			columna=columna[i]
			valor=valor[i]


			if valor == None:
					valor ="NULL"

			if i == 0 :
				valores= valor
				columnas = "\""+columna+"\""
			else:
				valores= valores+", "+str(valor)
				columnas = columnas+", \""+columna+"\""

			i=i+1
			valores,columnas = Utility.getValueAttr(datos,i,valores,columnas)

		return valores,columnas


	def descFuncion(function):

		if function == 'select':
			return "Esta función recibe una query y retorna una lista de objetos con los campos de la query"

		if function == 'all':
			return "Esta función retorna una lista de objetos"

		if function == 'filter':
			return "Esta función retorna una lista de objetos, recibe el nombre del campo a filtrar, ej: Persona().filter(nombre='Pepito',edad=13), solo es para filtros con and, para otros casos use where"

		if function == 'where':
			return "Esta función retorna una lista de objetos, recibe un string, ej: Persona().where( ' nombre=\"Pepito\" or nombre=\"Jaime\" ' )"

		if function == 'find':
			return "Obtiene un Objeto a partir del id"

		if function == 'save':
			return "Guarda un objeto, si el objeto no tiene atributo id o su atributo id=0 lo inserta, de lo contrario hace un update"

		if function == 'new':
			return "Es de uso interno, se usa en el save(), sirve para insertar un nuevo objeto"

		if function == 'print':
			return "Muestra todos los atributos y funciones de un objeto"

		return "Sin descripción"




	
