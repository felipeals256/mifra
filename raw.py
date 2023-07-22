import settings
from mifra.core.conexion import Conexion
from mifra.core.core import Core
from mifra.core.utility import Utility
from mifra.exodo import Exodo


class Raw(object):

	

	def query(query):

		con = Conexion(None)
		result=con.get(query)
		con.close()

		return [
        	dict(zip(con.encabezado, row))
        	for row in result
    	]

	def __init__(self):
		pass 

	











	

	

	


