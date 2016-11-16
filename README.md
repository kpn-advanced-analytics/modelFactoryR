# modelFactoryR
R package for Model Factory created by Advanced Analytics team at KPN.

Presentation about Model Factory can be found there: http://www.slideshare.net/MariaVechtomova/model-factory-presentation-meetup.
Detailed overview will follow soon.

### How to get started with your own Model Factory:

Note: in order to be able to use the package, you would need an Aster cluster.

1)  Install TeradataAsterR and set up Aster ODBC connection.

2)	Install the package (works with  R>=3.1.1). Note! R < 3.3 is recommended (otherwiese TeradataAsterR does not work properly)

3)	In R studio check your homedrive: Sys.getenv("HOMEDRIVE")

4)	In the repository you will find .kpnr.zip file. Unzip it. Put .kpnr folder that contains ONLY config.yaml file in your homedrive directory.

4)	In .kpnr update config.yaml file: put here your ODBC connection details.

5)	In the repository you will find an Aster_model_factory.sql file. Run the script in order to create schema's and tables so that you can use the Model factory

6)  You should be able to run the template file without any errors (it uses the dataset titanic.csv, which is located in folder data)

