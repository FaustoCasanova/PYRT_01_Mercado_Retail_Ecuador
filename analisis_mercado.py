# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 16:43:01 2023

@Author: Fausto Valentino Casanova Cedeño

        Indice:
            1) Import Data
            2) Cleaning Data
            3) Filter and Segment Data
            4) Detecting Missing Values
            5) Exploratory Analysis
                    --> Level 1 - A nivel de Ecuador
                    --> Level 2 - A nivel Provincia
                    --> Level 3 - A nivel Segmento - Provincias Pichincha y Guayas
                    --> Level 4 - Margenes Bruto y Neto del sector Retail 
                    
            6) Exportar Tablas
                                    
             
"""

# Libraries
import pandas as pd
import numpy as np
import os


# Set working directory
with open("PYRT_01_work_dir.txt", "r") as archivo:
    w_direct = archivo.read()

os.chdir(w_direct)



#-------------------------------------------------------------------------------------------------
                                        # Import data

# | Table | -> Codigos CIIU
ciiu = pd.read_csv("bi_ciiu.csv")

# | Table | -> Tipo de segmento en empresas
segmento = pd.read_csv("bi_segmento.csv") 

# | Table | -> Directorio de compañias
company = pd.read_csv("bi_compania.csv")  

# | Table | -> Datos de cada compañia
columnas = ['anio', 'expediente', "n_empleados", 'cod_segmento', 'ciiu_n1', 'ciiu_n6', 'ingresos_ventas', 'costos_ventas_prod', 'utilidad_neta']
ranking = pd.read_csv("bi_ranking.csv", usecols = columnas)

# | Table | -> Datos de las Provincias en Ecuador
prov_ec = pd.read_csv("prov_ecuador.csv")



#-------------------------------------------------------------------------------------------------
                                        # Cleaning data

# | Table | -> Codigos CIIU

ciiu["ciiu"] = ciiu["ciiu"].str.strip()
ciiu["descripcion"] = ciiu["descripcion"].str.capitalize()
ciiu["descripcion"] = ciiu["descripcion"].str.strip()
ciiu.columns = ciiu.columns.str.capitalize()


# | Table | -> Tipo de segmento en empresas

segmento["segmento"] = segmento["segmento"].str.strip() 
segmento["segmento"] = segmento["segmento"].str.capitalize()
segmento.columns = segmento.columns.str.capitalize()


# | Table | -> Directorio de compañias

strings = company.columns[company.dtypes == "object"]
for i in strings:
    company[i] = company[i].str.strip()
    company[i] = company[i].str.capitalize()
 
company.columns = company.columns.str.capitalize()
company.rename(columns={"Pro_codigo":"Provincia_id"}, inplace=True)


# | Table | -> Datos de cada compañia

ranking.rename(columns = {"anio":"Año", "cod_segmento":"Id_segmento"} ,
               inplace = True )
ranking.columns = ranking.columns.str.capitalize()    



#-------------------------------------------------------------------------------------------------
                                        # Filter - Segment and Merge data

# | Table | -> New Dataframe (data)

#---- Periodo de Analisis y Sector G47 Ventas al por Menor
periods = [2019,2020,2021,2022]
pattern = "^G47"

#---- Aplicar Filtros a la Tabla ranking
data = ranking[ranking["Año"].isin(periods) &
               ranking["Ciiu_n6"].str.contains(pattern) ].copy()


#---- Agregando columna Ciiu_3
data["Ciiu_n3"] = data["Ciiu_n6"].str.extract("(^G47.)") 
               

#---- Agregando company data
data = pd.merge(left  = data ,
                right = company.iloc[:,[0,1,4,5]],
                how   = "inner",
                on    = "Expediente"
                )
#---- Agregando segmento data
data = pd.merge(left  = data ,
                right = segmento,
                how   = "inner",
                on    = "Id_segmento"
                )
#---- Agregando prov_ec data
data = pd.merge(left = data ,
                right = prov_ec,
                how = "inner" ,
                on = "Provincia"
                ) 


#---- Reorganizar columnas
data.info()
data.drop(["Provincia_id_y"], axis=1, inplace=True)
data.rename(columns={"Provincia_id_x":"Provincia_id"}, inplace = True)

data = data.iloc[:,[0,16,11,12,14,15,1,10,5,13,3,6,9,7,2,8,4]]
data = data.sort_values(by=["Año","Region"])
data.reset_index(drop=True, inplace=True)



# | Table | -> Codigos CIIU

#---- Filtrar el Sector G47 Ventas al por Menor
pattern = "^G47"
ciiu = ciiu[ciiu["Ciiu"].str.contains(pattern)]


# Eliminar variables innecesarias del espacio de trabajo
del strings , periods , i , columnas , pattern



#-------------------------------------------------------------------------------------------------
                                        # Missing Values 

# | Table | -> Datos de cada compañia segmentado (data) 

#---- Global Missing values   
print(data.isna().sum(axis=0))

#---- Missiing values by Año
miss = data.groupby(["Año"], as_index=False)[["Costos_ventas_prod"]]


#---- Missing Value Function
def missing_values(df):
    # df -> groupby dataframe
    group = {}  
    for i , k in df:
        t = len(k)
        group[i] = pd.DataFrame(data=k.isna().sum(axis=0),columns=["Missing"])
        group[i]["%_from_total"] = round(group[i]["Missing"] / t , 3)
        
    output = pd.concat(group,axis=0)
    
    return print(output.to_markdown()) 

#---- Call the function     
missing_values(miss)


""" Observacion
        Dado que los valores perdidos de la columna "Costos_ventas_prod" 
        no afectan a los calculos previstos vamos a imputarlos con el valor "0"
"""        

#---- Imputando valores perdidos
data["Costos_ventas_prod"].fillna(0,inplace=True)

#---- Verificamos nuevamente
print(data.isna().sum(axis=0))



#-------------------------------------------------------------------------------------------------
                                        # Exploratory Analysis

# LEVEL 1 -->  A Nivel de Ecuador
    # 1.1) Empresas y Ventas en Millones USD en el sector Retail 
    # 1.2) Anexos para otros graficos



# | Analysis 1.1 | -> Empresas y Ventas en Millones USD en el sector Retail 

lev_1 = data.groupby(["Año"], as_index=False).agg({"Expediente"        :"count",
                                                   "Ingresos_ventas"   :"sum"  ,
                                                   }
                                              )
            
#---- Cambiando Escala de Ventas a (Millones USD)                                                    
lev_1[lev_1.columns[2:]] = round(lev_1[lev_1.columns[2:]]  / 1000000 , 2 )   

#---- Renombrando columnas
lev_1.rename(columns = {"Expediente":"Empresas",
                        "Ingresos_ventas":"Ventas_Mill_(Usd)"
                       } , 
             inplace = True)



# | Analysis 1.2 | -> Anexos para otros graficos

#---- | TreeMap data | -> Ventas en Millones USD por Clasificacion ciiu 3 (Long Format)  
    
#-------- Calculanto Ventas en millones por Año y Ciiu_3
tree_A = data.groupby(["Año","Ciiu_n3"], as_index = False)["Ingresos_ventas"].sum()

#-------- Agregando Descripcion a cada codigo ciiu_n3
tree_A = pd.merge(left     = tree_A,
                  right    = ciiu.iloc[:,[0,1]],
                  how      = "left" ,
                  left_on  = "Ciiu_n3",
                  right_on = "Ciiu"
                  )

#-------- Limpiando columna  Descripcion
tree_A[["Categoria", "Descripcion"]] = tree_A['Descripcion'].str.extract(r'(^Venta al por menor)(.*)')
tree_A["Categoria"] = "Retail"

#-------- Ajustando Formato en columnas
tree_A["Descripcion"] = tree_A["Descripcion"].str.strip()
tree_A["Descripcion"] = tree_A["Descripcion"].str.capitalize()
tree_A.drop(["Ciiu"],axis=1, inplace=True)

#-------- Ajustando Escala de valores 
tree_A["Ingresos_ventas"] = tree_A["Ingresos_ventas"] / 1000000
tree_A["Ingresos_ventas"] = tree_A["Ingresos_ventas"].round(2)
tree_A.rename(columns={"Ingresos_ventas":"Ventas_Mill_USD"}, inplace=True)

#-------- Ordenando columnas
tree_A = tree_A.iloc[:,[0,1,4,3,2]]



#---- | Table data | -> Ventas en Millones USD Sector Retail y Variacion YoY (Wide Format)

#-------- En Millones USD
table_w1 = pd.pivot(tree_A , index=["Ciiu_n3","Categoria","Descripcion"],
                     columns="Año",values="Ventas_Mill_USD").round(2).reset_index(drop=False)

#-------- Variacion YoY %
table_w2 = table_w1.copy()
table_w2.iloc[:,3:] = table_w1[table_w1.columns[3:]].pct_change(axis='columns', periods=1).round(2)




#-------------------------------------------------------------------------------------------------
                                        # Exploratory Analysis

# LEVEL 2 -->  A Nivel de Provincia
    # 2.1) Empresas y Ventas en Millones USD en el sector Retail 
    # 2.2) Anexos para otros graficos



# | Analysis 2.1 | -> Empresas y Ventas en Millones USD en el sector Retail (Long Format)

#---- Calculando Total ventas y Total empresas por Año y Provincia
lev_2 = data.groupby(["Año","Region","Provincia","Longitude","Latitude"] , 
                     as_index=False).agg(
                                         {"Expediente"     :"count",
                                          "Ingresos_ventas":"sum"
                                         }
                                    )

#---- Cambiando Escala de Ventas a (Millones USD) 
lev_2["Ingresos_ventas"] = round(lev_2["Ingresos_ventas"] / 1000000 , 3 ) 

#---- Renombrando columnas
lev_2.rename(columns = {"Expediente":"Empresas",
                        "Ingresos_ventas":"Ventas_Mill_(Usd)"
                        } , 
             inplace = True)


#---- Ordenando Tabla por Año y Region
lev_2 = lev_2.sort_values(by=["Año","Region"],ascending=True)



# | Analysis 2.2 | -> Anexos para otros graficos

#---- | Table data | -> Ventas en Millones USD a nivel de provincia (Wide Format)

#-------- Millones USD
table_w3 = pd.pivot(data = lev_2, index = "Provincia", columns = "Año", 
                    values = "Ventas_Mill_(Usd)").round(2).reset_index(drop=False)
table_w3 = table_w3.sort_values(by=2019, ascending=False)

#-------- YoY %
table_w4 = table_w3.copy()

table_w4.iloc[:,1:] = table_w4.iloc[:,1:].pct_change(axis="columns",periods=1).round(2)



#-------------------------------------------------------------------------------------------------
                                        # Exploratory Analysis

# LEVEL 3 -->  A nivel Segmento de las Provincias Pichincha y Guayas
    # 3.1) Empresas y Ventas en Millones USD en el sector Retail 


# | Analysis 3.1 | -> Empresas y Ventas en Millones USD en el sector Retail 

#---- Filtrando solo Provincias Guayas y Pichincha
lev_3 = data[data["Provincia"].isin(["Guayas","Pichincha"])]

#---- Calculando Total empresas y Ventas en Millones por Año Provincia y Segmento
lev_3 = lev_3.groupby(["Año","Provincia","Segmento"], 
                      as_index=False, dropna = True).agg(
                                                          {"Expediente"     :"count",
                                                           "Ingresos_ventas":"sum"
                                                           }
                                                      )
                          
#---- Cambiando Escala de Ventas a (Millones USD) 
lev_3["Ingresos_ventas"] = round(lev_3["Ingresos_ventas"] / 1000000 , 2 ) 

#---- Renombrando columnas
lev_3.rename(columns = {"Expediente":"Empresas",
                        "Ingresos_ventas":"Ventas_Mill_(Usd)"
                        } , 
             inplace = True)                                                           
                                                                                                               


#-------------------------------------------------------------------------------------------------
                                        # Exploratory Analysis

# LEVEL 4 -->  Margenes Bruto y Neto Ecuador y Provincias Pichincha y Guayas 
    # 4.1) Margen Bruto y Neto a nivel Ecuador
    # 4.2) Margen Bruto y Neto a nivel de Provincias Pichincha y Guayas 
    # 4.3) Margen Bruto y Neto a nivel de Provincias Pichincha y Guayas por Segmentos


# | Analysis 4.1 | -> Margen Bruto y Neto a nivel Ecuador

#---- Agrupando variables por Año
mrg_ec = data.groupby(["Año"], as_index = False).agg({"Ingresos_ventas":"sum",
                                                      "Costos_ventas_prod":"sum",
                                                      "Utilidad_neta":"sum"
                                                      }
                                                 )

#---- Margen Bruto A
mrg_ec["Margen_bruto"] = \
    round(
          (mrg_ec["Ingresos_ventas"] - mrg_ec["Costos_ventas_prod"] ) / 
           mrg_ec["Ingresos_ventas"] , 
           3
        )

#---- Margen Neto A
mrg_ec["Margen_neto"]  = round(mrg_ec["Utilidad_neta"] / mrg_ec["Ingresos_ventas"] , 3)



# | Analysis 4.2 | -> Margen Bruto y Neto a nivel de Provincias Pichincha y Guayas 

#---- Tabla temporal - data solo de 2 provincias 
temp = data[data["Provincia"].isin(["Guayas","Pichincha"])]

#---- Agrupando variables por Año y Provincia
mrg_pr = temp.groupby(["Año","Provincia"], as_index = False).agg({"Ingresos_ventas":"sum",
                                                                  "Costos_ventas_prod":"sum",
                                                                  "Utilidad_neta":"sum"
                                                                  }
                                                             )

#---- Margen Bruto B
mrg_pr["Margen_bruto"] = \
    round(
          (mrg_pr["Ingresos_ventas"] - mrg_pr["Costos_ventas_prod"] ) / 
           mrg_pr["Ingresos_ventas"] , 
           3
        )

#---- Margen Neto B
mrg_pr["Margen_neto"]  = round(mrg_pr["Utilidad_neta"] / mrg_pr["Ingresos_ventas"] , 3)



# | Analysis 4.3 | -> Margen Bruto y Neto a nivel de Provincias Pichincha y Guayas por segmento

#---- Agrupando variables por Año Provincia y Segmento    
mrg_sg = temp.groupby(["Año","Provincia","Segmento"], as_index = False).agg({"Ingresos_ventas":"sum",
                                                                             "Costos_ventas_prod":"sum",
                                                                             "Utilidad_neta":"sum"
                                                                             }
                                                                         )

#---- Margen Bruto C
mrg_sg["Margen_bruto"] = \
    round(
          (mrg_sg["Ingresos_ventas"] - mrg_sg["Costos_ventas_prod"] ) / 
           mrg_sg["Ingresos_ventas"] , 
           3
        )

#---- Margen Neto C
mrg_sg["Margen_neto"]  = round(mrg_sg["Utilidad_neta"] / mrg_sg["Ingresos_ventas"] , 3)




#-------------------------------------------------------------------------------------------------
                                        # Exportar data

"""

# Database                                        
data.to_csv("database.csv",index=False)

# Seccion 1
lev_1.to_csv("lev_1.csv",index=False)
tree_A.to_csv("tree_A.csv",index=False)
table_w1.to_csv("table_w1.csv",index=False)
table_w2.to_csv("table_w2.csv",index=False)

# Seccion 2
lev_2.to_csv("lev_2.csv",index=False)
table_w3.to_csv("table_w3.csv", index=False)
table_w4.to_csv("table_w4.csv", index=False)

# Seccion 3 
lev_3.to_csv("lev_3.csv", index=False)

# Seccion 4
mrg_ec.to_csv("mrg_ec.csv",index=False)
mrg_pr.to_csv("mrg_pr.csv",index=False)
mrg_sg.to_csv("mrg_sg.csv",index=False)

"""







