# modelFactoryR
R package for Model Factory created by Advanced Analytics team at KPN.

[What is Model Factory?](https://gist.github.com/kpn-advanced-analytics/13477b6d419531bc7232ef4da1a4cda2)

### How to get started with your own Model Factory:

Note: in order to be able to use the package, you would need an Aster cluster.

1)  Install TeradataAsterR and set up Aster ODBC connection.

2)	Install the package (works with  R>=3.1.1). Note! R < 3.3 is recommended (otherwiese TeradataAsterR does not work properly)

3) create MODELFACTORY environmental variable. On Windows it can be tricky, you need to do the following:

    -add a system environment variable MODELFACTORY with value of folder of your choice, for example: C:\Projects;

    -add the following line to PATH system environment variable: %MODELFACTORY%\bin;

    -in command line call echo %MODELFACTORY% -> this should return the specified path
  
4) Copy the config.yaml file that you can find in the repository in folder specified in MODELFACTORY (e.g., C:\Projects). Fill in the config.yaml file

5)	In the repository you will find an Aster_model_factory.sql file. Run the script in order to create schema's and tables so that you can use the Model factory

6)  You should be able to run the template file without any errors (it uses the dataset titanic.csv, which is located in folder data)

