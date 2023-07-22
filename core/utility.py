

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
				columnas = columna
			else:
				valores= valores+", "+str(valor)
				columnas = columnas+", "+columna

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




	