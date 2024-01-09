#python3 setup.py sdist

import setuptools

#Si tienes un readme
with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name='mifra',  #nombre del paquete
    version='0.1', #versión
    license='MIT',
    scripts=['exodo.py','log.py','raw.py','master.py'] , #nombre del ejecutable
    author="Felipe Leiva", #autor
    author_email="anexatec@gmail.com", #email
    description="Un package hacer migraciones", #Breve descripción
    long_description=long_description,
    long_description_content_type="text/markdown", #Incluir el README.md si lo has creado
    url="https://github.com/felipeals256/mifra", #url donde se encuentra tu paquete en Github
    #packages=setuptools.find_packages(), #buscamos todas las dependecias necesarias para que tu paquete funcione (por ejemplo numpy, scipy, etc.)
    #classifiers=[
    #    "Programming Language :: Python :: 3",
    #    "License :: OSI Approved :: MIT License",
    #    "Operating System :: OS Independent",
    #],
 ) #aquí añadimos información sobre el lenguaje usado, el tipo de licencia, etc.