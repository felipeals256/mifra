from settings import DB_LOG
from settings import _APP_ENV

from mifra.master import Master
from mifra.int.mail import Mail

import argparse
 
 



class LogHistory(Master):

	def __init__(self):
		self.info_log_id=LogHistory.get_id_log()

	def get_id_log():
		# Initialize parser
		parser = argparse.ArgumentParser()
		parser.add_argument("-idl", "--id_log", help = "Show Output")
		args = parser.parse_args()
		 
		try:
			return args.id_log
		except Exception as e:
			return None

	created_at	=None
	information =None #máximo 100
	step     	=None
	error       =None
	info_log_id =None

	def save(self,**kwargs):
		self.created_at='now()'
		self.information=None
		self.step=None
		if not self.info_log_id:
			self.info_log_id=LogHistory.get_id_log()
			if self.info_log_id == None:
				self.info_log_id=0

		if 'information' in kwargs:
			self.information=kwargs['information']

		if 'step' in kwargs:
			self.step=kwargs['step']

		if 'info_log_id' in kwargs:
			self.info_log_id=kwargs['info_log_id']

		if 'error' in kwargs:
			self.error=kwargs['error']


		super().save()





	class Meta:
		db_table ="info_log_history"
		db_conexion = DB_LOG

class Log(Master):

	created_at	=None
	process   	=None #máximo 100
	error     	=None
	message   	=None
	ambient   	=None
	detail      =None #(opcional) máximo 100
	finish_at   =None #Es la fecha cuando el proceso terminó
	step_finish =None #Hay procesos que tienen varios pasos, aquí puedo guardar la cantidad de pasos de un proceso


	step_current=0#Esto va autoincrementandose solito

	def current_save(self,**kwargs):
		log = self.first(id=LogHistory.get_id_log())

		if not log:
			return None

		if 'created_at' in kwargs:
			log.created_at=kwargs['created_at']

		if 'process' in kwargs:
			log.process=kwargs['process']

		if 'error' in kwargs:
			log.error=kwargs['error']

		if 'message' in kwargs:
			log.message=kwargs['message']


		if 'ambient' in kwargs:
			log.ambient=kwargs['ambient']

		if 'detail' in kwargs:
			log.detail=kwargs['detail']

		if 'finish_at' in kwargs:
			log.finish_at=kwargs['finish_at']

		if 'step_finish' in kwargs:
			log.step_finish=kwargs['step_finish']

		log.save()	
		return log


	def get_and_check_ambient():
		if _APP_ENV != 'TEST' and _APP_ENV != 'PROD' and _APP_ENV != 'DEV' and _APP_ENV != 'DEV_EXCEPT':
			print("settings.DB_LOG debe ser igual a TEST, PROD o DEV")
			exit()

		return _APP_ENV


	def __init__(self):
		pass

	def init(**kwargs):
		if not 'process' in kwargs:
			print("Ingrese el nombre del proceso. Ej Log.init(process='mi_proceso')")
			exit()

		log = Log()
		log.created_at 			  ='now()'
		log.process               =kwargs['process']
		log.error                 =True
		log.message               =Log.get_message_error()
		log.ambient               =Log.get_and_check_ambient()
		log.detail                =None
		log.more_information      =None
		log.step_finish 		  =None
		
		if 'detail' in kwargs:
			log.detail            =kwargs['detail']
		
		if 'more_information' in kwargs:
			log.more_information  =kwargs['more_information']
		
		if 'step_finish' in kwargs:
			try:
				log.step_finish            =int(kwargs['step_finish'])
			except Exception as e:
				print("El valor del paso final debe ser numérico (step_finish)")
				exit()

		if 'message' in kwargs:
			log.message            =kwargs['message']

		if 'error' in kwargs:
			log.error            =kwargs['error']

		if 'ambient' in kwargs:
			log.ambient            =kwargs['ambient']
			
		
		if _APP_ENV != 'DEV':
			log.save()

		


		return log


	def step(self,**kwargs):

		history = LogHistory()
		history.created_at 			 ='now()'
		history.information          =None
		history.step                 =self.step_current+1
		history.info_log_id          =self.id
		
		if 'information' in kwargs:
			history.information            =kwargs['information']
		
		if 'step' in kwargs:
			history.step  = kwargs['step']
		

		self.step_current=history.step
		
		if _APP_ENV != 'DEV':
			history.save()


	def finish(self,**kwargs):
		self.finish_at 			  ='now()'
		self.error                 =False
		self.message               =Log.get_message_finish()		

		if 'error' in kwargs:
			self.error  =kwargs['error']
			if self.error:
				self.message  =Log.get_message_error()	
			else:	
				self.message  =Log.get_message_finish()

		if 'detail' in kwargs:
			self.detail            =kwargs['detail']
		
		if 'more_information' in kwargs:
			self.more_information  =kwargs['more_information']

		if 'message' in kwargs:
			self.message  =kwargs['message']

		
		
		if _APP_ENV != 'DEV':
			self.save()

		if self.error and 'email' in kwargs:
			texto="""
				<p>Un script no se ejecutó bien, a continuación más infomación</p>
				<table border="1">
					<tr>
						<td><b>FECHA</b></td>
						<td>{created_at}</td>
					</tr>
					<tr>
						<td><b>PROCESO</b></td>
						<td>{proceso}</td>
					</tr>
					<tr>
						<td><b>ERROR</b></td>
						<td>{error}</td>
					</tr>
				</table>
			""".format(proceso=self.process,error=self.more_information,created_at=str(self.created_at))


			Mail().enviar( asunto  ="ERROR EN EJECUCIÓN SCRIPT"#recibe un str con el texto del asunto
							, texto   =texto    #recibe un str con el texto del mensaje
							, destino  =kwargs['email'] #recibe una lista de destinos
						 )

		return self


	def get_message_error():
		return "El proceso no se completo"

	def get_message_finish():
		return "El proceso terminó correctamnete"


	class Meta:
		db_table ="info_log"
		db_conexion = DB_LOG















	

	

	


