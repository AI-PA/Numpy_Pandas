#Mandar llamar librerias básicas
library(data.table)
library(plyr)
library(dplyr)

#Deshabilitar notación científica
options(scipen = 999)

#Abrir bases de datos
BD_CATALOGO <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_CATALOGO.csv")
BD_CATALOGO_2 <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_CATALOGO_2.csv")
BD_FINANZAS <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_FINANZAS.csv")
BD_REGION <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_REGION.csv")
BD_RESPONSABLES <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_RESPONSABLES.csv")
BD_SISTEMAS <- fread("C:/Users/SALA 04/Desktop/Ejercicio_R/Ejercicio_R/BD_SISTEMAS.csv")

#Anexar BD_CATALOGO con BD_CATALOGO_2
BD_CATALOGo_COMPLETO<- rbind(BD_CATALOGO,BD_CATALOGO_2)

#Renombrar columnas
colnames(BD_CATALOGo_COMPLETO)[1] <- "ID_PIEZA"
colnames(BD_CATALOGo_COMPLETO)[2] <- "NOMBRE_PIEZA"
colnames(BD_CATALOGo_COMPLETO)[3] <- "ID_SISTEMA"
colnames(BD_CATALOGo_COMPLETO)[4] <- "ID_RESPONSABLE"
colnames(BD_CATALOGo_COMPLETO)[5] <- "PIEZA_HERMANA"
colnames(BD_CATALOGo_COMPLETO)[6] <- "ID_REGION"
colnames(BD_CATALOGo_COMPLETO)[7] <- "TIEMPO_PROCESAMIENTO_PROMEDIO"
colnames(BD_CATALOGo_COMPLETO)[8] <- "DEMANDA"
colnames(BD_CATALOGo_COMPLETO)[9] <- "TIPO_PROCESO"
colnames(BD_CATALOGo_COMPLETO)[10] <- "CODIGO_MAQUINA"
colnames(BD_RESPONSABLES)[3] <- "ID_RESPONSABLE_2"
colnames(BD_REGION)[3] <- "ID_REGION_2"


#Quitar duplicados por un campo
BD_CATALOGo_COMPLETO <- BD_CATALOGo_COMPLETO[!duplicated(BD_CATALOGo_COMPLETO[ , c("ID_PIEZA")]),]

#Combinar consultas BD CATALOGO CON EL RESTO DE TABLAS. 
BD_CATALOGo_COMPLETO <- merge(BD_CATALOGo_COMPLETO,BD_SISTEMAS,by="ID_SISTEMA",all.x =T)
BD_CATALOGo_COMPLETO <- merge(BD_CATALOGo_COMPLETO,BD_RESPONSABLES,by="ID_RESPONSABLE",all.x =T)
BD_CATALOGo_COMPLETO <- merge(BD_CATALOGo_COMPLETO,BD_REGION,by="ID_REGION",all.x =T)
BD_CATALOGo_COMPLETO <- merge(BD_CATALOGo_COMPLETO,BD_FINANZAS,by="ID_PIEZA",all.x =T)

#Ordenar y quitar columnas
BD_CATALOGo_COMPLETO <-  BD_CATALOGo_COMPLETO[,c("ID_PIEZA",
             	                                   "NOMBRE_PIEZA",
             	                                   "SISTEMA_DESC",
                                                 "RESPONSABLE_DESC",
                                                 "REGION_DESC",
                                                 "TIEMPO_PROCESAMIENTO_PROMEDIO",
                                                 "DEMANDA",
                                                 "TIPO_PROCESO",
                                                 "CODIGO_MAQUINA",
                                                 "COSTO",
                                                 "PRECIO")]

#Filtrando Proceso Tipo 1
BD_CATALOGo_COMPLETO <- BD_CATALOGo_COMPLETO[BD_CATALOGo_COMPLETO$TIPO_PROCESO == "1",]

#Sacando relacion de piezas donde el costo es mas elevado que el precio
BD_CATALOGo_COMPLETO$VALIDACION <- ifelse (BD_CATALOGo_COMPLETO$COSTO > BD_CATALOGo_COMPLETO$PRECIO, "CARO", "BARATO")

#Filtrando Proceso Piezas Caras
BD_PIEZAS_CARAS <- BD_CATALOGo_COMPLETO[BD_CATALOGo_COMPLETO$VALIDACION == "CARO",]

#Agrupar por el Sistema para hacer el conteo de las piezas. 
PIEZAS_X_SISTEMA <- BD_CATALOGo_COMPLETO %>% group_by(SISTEMA_DESC) %>% summarise(CONTEO=n(),.groups = "drop")

#Cuantas piezas son importadas por cada pais (REGION_DESC) 
PIEZAS_X_PAIS <- BD_CATALOGo_COMPLETO %>% group_by(REGION_DESC) %>% summarise(CONTEO=n(),.groups = "drop")

#Responables que no tiene una pieza asignada. 
RESPONSABLE_SIN_PIEZA <- merge(BD_RESPONSABLES,BD_CATALOGo_COMPLETO,by="RESPONSABLE_DESC", all.x=T)

#Reemplazar valores nulos por 0 (ID_PIEZA)
RESPONSABLE_SIN_PIEZA[which(is.na(RESPONSABLE_SIN_PIEZA$ID_PIEZA)),"ID_PIEZA"] <-0

#Filtrando Responsables que no tienen piezas
RESPONSABLE_SIN_PIEZA <- RESPONSABLE_SIN_PIEZA[RESPONSABLE_SIN_PIEZA$ID_PIEZA == 0,]

#IDENTIFICAR EL TIPO DE MAQUINA, POR EL PRIMER DIGITO.
BD_CATALOGo_COMPLETO$MAQUINA <-substr(BD_CATALOGo_COMPLETO$CODIGO_MAQUINA,1,1)

#Obteniendo el tiempo total por cada maquina.
TIEMPOS_X_MAQUINA <-BD_CATALOGo_COMPLETO %>% group_by(MAQUINA) %>% summarise(SUM= sum(TIEMPO_PROCESAMIENTO_PROMEDIO))


#Escribir archivo
fwrite(BD_CATALOGo_COMPLETO,"BD_CATALAGO_COMPLETO.CSV",sep=",",append=F,quote=F,row.names=F,col.names=T)
fwrite(BD_PIEZAS_CARAS,"PIEZAS_CARAS.txt",sep="|",append=F,quote=F,row.names=F,col.names=T)
fwrite(PIEZAS_X_SISTEMA,"PIEZAS_x_SISTEMA.txt",sep="|",append=F,quote=F,row.names=F,col.names=T)
fwrite(PIEZAS_X_PAIS,"PIEZAS_X_PAIS.txt",sep="|",append=F,quote=F,row.names=F,col.names=T)
fwrite(RESPONSABLE_SIN_PIEZA,"RESPONSABLES_SIN_PIEZA.txt",sep="|",append=F,quote=F,row.names=F,col.names=T)
fwrite(TIEMPOS_X_MAQUINA,"TIEMPOS_x_MAQUINA.txt",sep="|",append=F,quote=F,row.names=F,col.names=T)