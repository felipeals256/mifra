
from mifra.core.conexion import Conexion
from mifra.core.core import Core
from mifra.exodo import Exodo

class Temp():
	class Meta:
		db_conexion = None


class Raw(object):

	con=None

	def query(query,**kwargs):
		objeto=None
		if "con" in kwargs:
			objeto=Temp()
			objeto.Meta.db_conexion=kwargs['con']
		else:
			print("Ingresa la conexion")
			exit()


		con = Conexion(objeto)
		result=con.get(query)
		con.close()

		return [
        	dict(zip(con.encabezado, row))
        	for row in result
    	]

	def execute(query,**kwargs):
		objeto=None
		if "con" in kwargs:
			objeto=Temp()
			objeto.Meta.db_conexion=kwargs['con']

		con = Conexion(objeto)
		result=con.execute(query)
		con.close()


	def exec(self,query,**kwargs):
		objeto=None
		if "con" in kwargs:
			objeto=Temp()
			objeto.Meta.db_conexion=kwargs['con']
			
		self.con = Conexion(objeto)
		self.con.set_cursor()
		self.con.cursor.execute(query)

		if not "con_close" in kwargs:
			self.commit()
			self.con_close()

		return self

	def get(self):
		encabezado = self.con.cursor.description
		columns = [col[0] for col in self.con.cursor.description]
		rows = [dict(zip(columns, row)) for row in self.con.cursor.fetchall()]
		return rows

	def commit(self):
		self.con.conn.commit() #hace el commit

	def con_close(self):
		self.con.close()

	def __init__(self):
		pass 
		
