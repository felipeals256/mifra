import settings #archivo de settingsuración de la app

from email import encoders                                                                       
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename
import smtplib
import ssl



class Mail:

	def __init__(self):

		self.remitente          = settings.EMAIL_SMTP_USER
		self.password           = settings.EMAIL_SMTP_PASS
		
	
		


	def enviar(self
		, asunto  ="UACH"#recibe un str con el texto del asunto
		, texto   =""    #recibe un str con el texto del mensaje
		, destino  =None #recibe una lista de destinos
		, archivos= None #recibe una lista con la rura de los archivos
		):

	

		if(settings.APP_ENV=="TEST"):
			destino =settings.EMAIL_DAFAULT_ADDRESS

		msg = MIMEMultipart()
		msg['From'] = self.remitente
		msg['To'] = ', '.join(destino)
		msg['Subject'] = asunto

		
		msg.attach(MIMEText(texto, 'html'))

		
		#try:
		smtp = smtplib.SMTP(
					host = settings.EMAIL_SMTP_SERVER,
					port = settings.EMAIL_SMTP_PORT
				)
		smtp.starttls(context=ssl.create_default_context())  
		smtp.login(settings.EMAIL_SMTP_USER,settings.EMAIL_SMTP_PASS)
		smtp.sendmail(self.remitente, destino, msg.as_string())

		#except:
		#	print("Error al tratar de enviar correo con usuario y clave a "+str(destino))
		#	return False

		return True