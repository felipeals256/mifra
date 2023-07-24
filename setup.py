
#python setup.py sdist
from setuptools import setup
setup(
    name="mifra",
    version="1.0",
    description="Este paquete sirve para obtener objetos de una base de datos",
    author="Felipe Leiva",
    author_email="anexatec@gmail.com",
    url="https://www.anexatec.cl",
    packages=['mifra','mifra.int','mifra.core'],
    scripts=[]
)
