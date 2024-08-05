import shutil
import os,sys
import teradatasql
import logging
from datetime import datetime
import getConfig as gc
import pandas as pd
import csv
from ftplib import FTP
import pysftp
import re
import xlsxwriter
import Frame_Util as fg

fec1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha=datetime.now().strftime("%Y%m%d_%H%M%S")
fecini = datetime.now().strftime("%Y%m%d")
horaini = datetime.now().strftime("%H%M%S")

cntd = curtd = None

def run(xlayout=None, xFechaIni=None, xFechaFin=None):
    try:
        if xlayout == None: # len(sys.argv) <2:
            print("Parametro no Ingresado...!")
            print("Uso: python {0} nombLayout YYYYMMDD".format(sys.argv[0]))
            sys.exit()
        xNomLayout = xlayout #sys.argv[1]

        FileLog = os.path.join(gc.path_l, "EXTDT_"+ xNomLayout +"_"+ fecha + ".log")
        programa = sys.argv[0]
        programa = programa.split("/")[-1]
        fg.regFileLog(FileLog, programa)
        os.chdir(gc.path_o)
        
        #host, username, password = gc.hostd, gc.usrtd, gc.pwdtd
        logging.debug('Inicio de programa')
        logging.info('Inicio conexion a Teradata...')
        
        #conectarse a Teradat
        global cntd, curtd
        cntd = ConnectTD()
        curtd = cntd.cursor()

        # Obtencion de parametros Fecha Inicio y Fecha Fin
        sql_stm = """SELECT Parametro FROM {}.VW_LAYOUT_EXTR
        WHERE NombLayout = '{}'""".format(gc.bdcfg,xNomLayout)
        df_par = read_sql(sql_stm,cntd)

        xParametroFecha = df_par["Parametro"][0]        
        xParFecIni = xParametroFecha.split("|")[0].replace(" ","")        
        xParFecIni = xParFecIni.replace("{FecIni}=","").replace("NOW()","DATE")        
        xParFecFin = xParametroFecha.split("|")[1].replace(" ","")
        xParFecFin = xParFecFin.replace("{FecFin}=","").replace("NOW()","DATE")        

        indFileOut = 0

        if xFechaIni == None:
           print("Parametro de Fecha Inicio no ingresado...!")
           logging.info("Parametro de Fecha Inicio no ingresado...!")
           print("Se asigna parametro por defecto desde campo Parametro en VW_LAYOUT_EXTR")
           logging.info("Se asigna parametro por defecto desde campo Parametro en VW_LAYOUT_EXTR")
           sql_stm = "select {} Fecha".format(xParFecIni)
           dffec = read_sql(sql_stm, cntd)
           xFechaIni = "'" + str(dffec["Fecha"][0]) + "'"
           #xFechaIni = xParFecIni
           print("Fecha Ini :: " + str(xFechaIni))
           logging.info('Fecha Ini :: '+ str(xFechaIni))
        else:
           xFechaIni = "'" + xFechaIni + "'"
           indFileOut = 1

        if xFechaFin == None:
           print("Parametro de Fecha Fin no ingresado...!")
           print("Se asigna parametro por defecto desde campo Parametro en VW_LAYOUT_EXTR")
           sql_stm = "select {} Fecha".format(xParFecFin)
           dffec = read_sql(sql_stm, cntd)
           xFechaFin = "'" + str(dffec["Fecha"][0]) + "'"
           #xFechaFin = xParFecFin
           print("Fecha Fin :: " + str(xFechaFin))
           logging.info('Fecha Fin :: '+ str(xFechaFin))
        else:
           xFechaFin = "'" + xFechaFin + "'"

        # Validamos si proceso fue ejecutado desde Schedule
        tipSchd = 'ExTdt'
        cFile = "Prc_{}_{}.prc".format(tipSchd,xlayout)
        schdMatrixCD = fg.ValidaSchedule(cFile)
        logging.info('schdMatrixCD = {}'.format(schdMatrixCD))


        # -------    Programa Principal    ------ #
        sql_stm= """SELECT * FROM {}.VW_LAYOUT_EXTR
        WHERE NombLayout = '{}'""".format(gc.bdcfg,xNomLayout)
        df = read_sql(sql_stm,cntd)
        fec1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_stm = """SELECT COALESCE(Max(logId)+1,1) FROM {}.TB_LOG_LAYOUT_EXTR""".format(gc.bdcfg)
        logId = read_sql2(sql_stm,curtd)
        if len(df) < 1:
           raise Exception("Error en Nombre de Layout {}...!".format(xlayout))

        layoutExtrID = int(df["LayoutExtrID"][0])
        estadoIni = 32       
        lista = [logId, layoutExtrID, xNomLayout, fec1, estadoIni]
        regLog= regStartExtrTD(curtd, lista)
        
        sep = str(df["separador"][0])
        #print("sep: {}     XX:{}".format(sep,df["separador"][0]))
        frmOut = df["formatoSalida"][0]
        vista = df["NombObjeto"][0]
        parametros = df["Parametro"][0]
        cwhere = df["condicionWhere"][0]

        cwhere = cwhere.replace("{FecIni}",xFechaIni)
        cwhere = cwhere.replace("{FecFin}",xFechaFin)

        #print("where :: " + cwhere)

        rutaCD = df["RutaCD"][0]
        sql_stm = """SELECT  rutaCD, Ruta, IPServidor ip
        , puerto, UsrServidor usr, PassServ pwd
        FROM {}.VW_RUTAS where RutaCD = {}""".format(gc.bdcfg,rutaCD)

        dff = read_sql(sql_stm,cntd)   # DataFrame de DatosFTP

        fileOut = df["preFijoLayout"][0]

        sql_stm = "Select {} sub".format(df["subFijoLayout"][0])
        dfs = read_sql(sql_stm,cntd)

        #xsub = ""

        #if len(str(df["subFijoLayout"][0]))>1:

        #   try: 
        #        sql_stm = "Select {} sub".format(df["subFijoLayout"][0])
        #        dfs = read_sql(sql_stm,cntd)
        #        xsub =  dfs["sub"][0]
        #   except:
        #        xsub = ""
        #   fileOut = "{}/{}{}.{}".format(gc.path_o,df["preFijoLayout"][0],\
        #           xsub,df["extension"][0])
 
        if (indFileOut == 0):
           xSufijo = dfs["sub"][0]
           fileOut = "{}/{}{}.{}".format(gc.path_o,df["preFijoLayout"][0],\
                   xSufijo,df["extension"][0])
        else:
            xSufijo = xFechaIni.replace("'","").replace("-","")
            fileOut = "{}/{}{}.{}".format(gc.path_o,df["preFijoLayout"][0],\
                   xSufijo,df["extension"][0])
            
        indHeader = (df["indHeader"][0] == 1)
        cab = str(df["camposLayoutCab"][0])
        campos = df["CamposLayout"][0]
        #filename = "{}{}.{}".format(df["preFijoLayout"][0], dfs["sub"][0],df["extension"][0])
        #filename = "{}{}.{}".format(df["preFijoLayout"][0], xsub,df["extension"][0])
        filename = "{}{}.{}".format(df["preFijoLayout"][0], xSufijo, df["extension"][0])
       
        if (cab != "None" and len(sep) > 1):
            cabecera = [cab.replace(",",sep)]   #sep
        elif(cab != "None" and len(sep) == 1):
            cabecera = cab.split(",")
        elif(cab == "None" and len(sep) > 1):
            cabecera = [campos.replace(",",sep)]
        else:
            cabecera = campos.split(",")
        
        ctl = df["EstructuraCTL"][0]
        
        # Validamos si debemos ejecutar Proceso previo
        if (df["procesoPrevio"][0] is not None):
            # Ejecuta Store Procedure
            sp = df["procesoPrevio"][0]
            logging.info("Ejecutando SP: {}".format(sp))
            cur_execute(curtd,"call "+sp)
        # Formato de Salida de Archivo
        if (frmOut == "Ascii/Plano"):
            lenSep = len(sep)
            if len(sep) > 1:
                campos = df["CamposLayout"][0].replace(",","||'"+sep+"'||")
                sep = "|"
            
            logging.info('Generando Exteractor: ')
            qry = "Select {0} From {1} where {2}".format(campos,vista,cwhere)
            logging.info("aqui")
            #print(qry)
            genera_csv2(curtd, qry, fileOut, indHeader,cabecera,sep,ctl,lenSep)
        
        elif (frmOut == "Excel"):
            qry = "Select {0} From {1} where {2}".format(campos,vista,cwhere)
            genera_xlsx(curtd, qry, fileOut, indHeader,cabecera)
        
        logging.info('Fin de generación de Extractor...')

        #  Validacion de Ejecucion de Proceso Posterior
        tipoProcesoPost = df["tipoProcPost"][0]
        procesoPost = df["procesoPost"][0]
                
        xComandoSO = str(procesoPost) + " " + str(fileOut)
        
        if (tipoProcesoPost == "Compresion"):
           os.system(xComandoSO)
           fileOut = str(fileOut) + ".gz"

        #Registrar FIN LOG Extractor
        estadoFin = 33
        tipejec = "M"
        fec1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lista = [fec1,estadoFin, tipejec, logId ]
        regLog= regEndExtrTD(curtd, lista)
        
        #shutil.move(fileOut, gc.path_i)
        ip= str(dff["ip"][0])
        ruta=str(dff["Ruta"][0])
        ruta = "{}/{}".format(ruta,filename)
        if (ip == gc.iploc) :
            logging.info('Iniciando transferencia a : {} ...'.format(ruta))
            shutil.copyfile(fileOut,ruta)
            logging.info('Fin  transferencia OK')
        else:
            logging.info('Iniciando FTP ...')
            putFileFTP(curtd,dff,fileOut,logId)
        
        # Envia Correo
        # EnviaCorreo(cTO, cCC, cAsunto,cMsj, cListAdjunto=[]):
        print("Llego aqui:", df['Correo'][0])
        cTO = df['Correo'][0]
        if cTO:
           cAsunto = "[OK] Extractor {}".format(xNomLayout)
           cCC = cTO
           cMsj = df['MsjCorreo'][0]
           cListAdjunto=[FileLog]
           size = float(os.path.getsize(fileOut))/1000000.0           
           #if os.path.getsize(fileOut) < 10240:  # < 10MB
           if size <= 24:  # 24MB
              cListAdjunto.append(fileOut)
           #fg.EnviaCorreo(cTO, cCC, cAsunto,cMsj, cListAdjunto)
           fg.EnviaEmail(cTO, cAsunto, cMsj, cListAdjunto)

        # SI fue lanzado por Schedule Actualizamos LOG de Schedule
        vOk = True
        if (schdMatrixCD != None):
           #ACtualizamos Log Schedule
           fg.regEndOkJob(cntd,schdMatrixCD)
        
    except:
        logging.error("2: MSG: %s", sys.exc_info()[0:])
        estadoFin = 34
        tipejec = "M"        
        fec1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lista = [fec1,estadoFin, tipejec, logId ]
#        regLog= regEndExtrTD(cur_execute, lista)
        vOk = False
        if (schdMatrixCD != None):
          #ACtualizamos Log Schedule
          fg.regEndErrJob(cntd,schdMatrixCD)
        # EnviaCorreo(cTO, cCC, cAsunto,cMsj, cListAdjunto=[]):
        cTO = df['CorreoERR'][0]
        if cTO:
           cAsunto = "[ERROR] Extractor {}".format(xNomLayout)
           cCC = cTO
           cMsj = df['MsjCorreo'][0]
           cListAdjunto=[FileLog]
           #fg.EnviaCorreo(cTO, cCC, cAsunto,cMsj, cListAdjunto)
           fg.EnviaEmail(cTO, cAsunto, cMsj, cListAdjunto)
        connect.close()
    
    finally:
        cfile ="{}/{}".format(gc.path_t,cFile)
        if os.path.exists(cfile):
           os.remove(cfile)
        
        if vOk:
           logging.info('Fin de Ejecución OK...' )
        else:
           logging.error('Fin de Ejecución con ERROR...' )
        cntd.close()


def ConnectTD():
    try:
        logging.info('Conectandose a Terada...')
        connect = teradatasql.connect(None, host = gc.hostd, user = gc.usrtd, password = gc.pwdtd)
        logging.info('Conexion EXITOSA..!.')
        return connect
    except:
        logging.error('No se ha podido realizar la conexion.')
        logging.error("%s", sys.exc_info()[0:])
        raise Exception("Error en ConnectTD()...!")

def read_sql(query,connect):
    try:
        logging.debug("2: Extrayendo informacion:\n{}".format(query))
        df = pd.read_sql(query,connect)
        return(df)
    except:
        logging.error('2: Error en read_sql.\n',query)
        logging.error("2: MSG: %s", sys.exc_info()[0:])
        raise Exception("Error en read_sql()...!")


def read_sql2(query, curtd):
   try:
      curtd.execute(query)
      row = curtd.fetchone()
      return(row[0])
   except:
      logging.warning('Error en read_sql.\n',query)
      logging.error("Error inesperado: %s", sys.exc_info()[0:])
      raise Exception("Error en read_sql2()...!")

def DisplayResults (cur):
    while True:
        print (" === metadata ===")
        print ("  cur.rowcount:", cur.rowcount)
        print ("  cur.description:", cur.description)

        print (" === result ===")
        [ print (" ", row) for row in cur.fetchall () ]

        if not cur.nextset ():
            break

def cur_execute (cur, sSQL, params=None):
    print ()
    print ("cur.execute", sSQL, "bound values", params)
    cur.execute (sSQL, params)
#    DisplayResults (cur)

def cur_callproc (cur, sProcName, params=None):
    print ()
    print ("cur.callproc", sProcName, "bound values", params)
    cur.callproc (sProcName, params) # OUT parameters are not supported by .callproc
 #   DisplayResults (cur)

def genera_xlsx(curtd, qry, fileOut, indHeader,cabecera):
    try:
        wb = xlsxwriter.Workbook(fileOut)
        ws = wb.add_worksheet()
        logging.info("5: Extrayendo informacion de BD ...")
        curtd.execute(qry)
        row = curtd.fetchone()
        col = [i[0] for i in curtd.description]
        ln = len(col)

        if indHeader: # add column headers if requested
            for i in  range(ln):
                ws.write(0, i, col[i])
        lin = 1

        while row:
            for i in range(ln):
                ws.write(lin, i, row[i])
            row = curtd.fetchone()
            lin = lin+1
            #if (lin % 10000) == 0:
                #print(datetime.now().strftime("%Y%m%d %H%M%S")," Procesando Registros ", lin)
        wb.close()
        logging.info("5: Se genero el archivo correctamente ...")
    except:
        logging.error('Hubo un error al ejecutar:  genera_xlsx')
        logging.error(sys.exc_info()[0:])


def genera_csv (curs, cqry, cfile, printHeader, cCab, sep, ctl):
# Cursor, query, fileOut, IndPrintHeader, CabeceraFormat
  try:
    csv.register_dialect('unixpwd', delimiter=':', quoting=csv.QUOTE_MINIMAL)
    csv_file_dest = cfile
    ctl_file = "{}.ctl".format(cfile.split(".")[0])
    outputFile = open(csv_file_dest,"w") # 'wb'
    ctlFile = open(ctl_file,"w", encoding='latin-1')
    output = csv.writer(outputFile, delimiter=sep, quotechar='"',  dialect='excel')
    logging.info("3: Extrayendo informacion de BD ...")
    curs.execute(cqry)
    lin = 0

    if printHeader: # add column headers if requested
       if (cCab is None):
          cCab  = []
          for col in curs.description:
             cCab.append(col[0])

       output.writerow(cCab)

    for row_data in curs: # add table rows
      output.writerow(row_data)
      lin = lin+1

    ctlFile.write(str(lin)+"\n")
    outputFile.close()
    ctlFile.close()
    curs.close()
    logging.info("3: Archivo Generado: <<%s>>,  Lineas: <<%s>>",cfile, lin)
  except:
    logging.error("3: Error inesperado: %s", sys.exc_info()[0:])

def genera_csv2(curs, cqry, cfile, printHeader, cCab, sep, ctl,lenSep):
# Cursor, query, fileOut, IndPrintHeader, CabeceraFormat
  try:
    csv.register_dialect('unixpwd', delimiter="|", quoting=csv.QUOTE_MINIMAL)
    csv_file_dest = cfile
    logging.debug("3: Extraye")
    ctl_file = "{}.ctl".format(cfile.split(".")[0])
    out_file = "{}".format(cfile.split("/")[-1])
    outputFile = open(csv_file_dest,"w",encoding='latin-1') # 'wb'  
    ctlFile = open(ctl_file,"w", encoding='UTF-8')
    if lenSep == 1:
      output = csv.writer(outputFile, delimiter=sep, quotechar='"',lineterminator='\n', dialect='excel')
    else:
      output = csv.writer(outputFile, delimiter=sep,quoting=csv.QUOTE_NONE, escapechar="\\", lineterminator='\n', dialect='excel')
    logging.debug("3: Extrayendo informacion de BD ...\n{0}".format(cqry))
    curs.execute(cqry)

    if printHeader: # add column headers if requested
       xcab = cCab   #[cCab]
       output.writerow(xcab)
    lin = 0
    while True:
      rows = curs.fetchmany(1000)
      if not rows:
        break

      output.writerows(rows)
      lin = lin + len(rows)

    fecfin = datetime.now().strftime("%Y%m%d")
    horafin = datetime.now().strftime("%H%M%S")
    currentTimeStamp = datetime.now().strftime("%Y%m%d%H%M%S")

    ctl = ctl.replace("{CantidadRegs}",str(lin)).replace("{FecIni}",fecini).replace("{HoraIni}",horaini)\
    .replace("{FecFin}", fecfin).replace("{HoraFin}", horafin).replace("{NombArchivo}", str(out_file))\
    .replace("{CurrenTimestamp}",currentTimeStamp)
    ctlFile.write(str(ctl)+"\n")
    outputFile.close()
    ctlFile.close()
    curs.close()
    logging.info("3: Archivo Generado: <<%s>>,  Lineas: <<%s>>",cfile, lin)
  except:
    logging.error("3: Error inesperado: %s", sys.exc_info()[0:])

def putFileFTP(curtd,dff,xfile,logId):
   #print("dff: ",dff)
   server = dff["ip"][0]
   puerto = dff["puerto"][0]
   usr = dff["usr"][0]
   pwd = dff["pwd"][0]
   pwd  = fg.desencripta(pwd).decode("utf8")
   passd = pwd  # dff["pwd"][0]
   ruta = dff["Ruta"][0]
   filematch = "{}.".format((xfile.split(".")[0]).split("/")[-1])
   #print(filematch)
   size = os.path.getsize(xfile)
   try:
      dIni = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      filelist = os.listdir()
      estadoFTP = 2   # En proceso

      lista = [dIni,estadoFTP,xfile,size,logId]
      regStartFtpTD(curtd,lista)
      if puerto is None:
         puerto = '21'
      logging.info("5: Conectandose a: {}:{}".format(server,puerto))
      if puerto == "22": #Ftp Segurp SSH
         cnopts = pysftp.CnOpts()
         cnopts.hostkeys = None
         sftp = pysftp.Connection(server, username=usr, password=passd, cnopts=cnopts)
         logging.info("5a:3 ...Conexion establecida")
         sftp.cwd(ruta)             # Path Remote
         for filename in filelist:
            filedate = re.search(filematch, filename)
            if filedate:
               logging.info("5a:...Transfiriendo " + filename)
               sftp.put(filename,preserve_mtime=True)
               fdStat = sftp.stat(filename)
               #print("File: {}  Size: {}".format(filename,fdStat.st_size))
               logging.info("5a:...Transferencia finalizada")
               dFin =datetime.now().strftime("%Y-%m-%d %H:%M:%S")
               result=str(filename)
               logging.info(result)

         #Registrando Log en Teradata
      else:
         ftp = FTP(server)
         ftp.connect(server,int(puerto))
         ftp.login(usr,passd)
         logging.info("5b:...Conexion establecida")
         ftp.cwd(ruta)
        # print("Ruta lectura:", os.getcwd())
         for filename in filelist:
            filedate = re.search(filematch, filename)
            if filedate:
               dIni = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
               #fhandle = open(filename, 'wb')
               logging.info("5b:...Transfiriendo " + filename) #for confort sake, shows the file that's being retrieved}
               ftp.storbinary('STOR ' + filename, open(filename, 'rb'))
               logging.info("5b:...Transferencia finalizada")
               dFin =datetime.now().strftime("%Y-%m-%d %H:%M:%S")
               result=str(filename)
               logging.info(result)
               #Registrando Log en Teradata
      estadoFTP = 3
      lista = [dFin,estadoFTP,logId]
      regEndFtpTD(curtd,lista)

   except:
      logging.error("5: %s", sys.exc_info()[0:])
      msgErr = str(sys.exc_info()[0:]).replace("'","|")
      dFin =datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      lista = [dFin,4,msgErr, logId]
      regErrFtpTD(curtd, lista)
      raise

def regStartExtrTD(curtd, lista):
    try:
        stm_sql = ("INSERT INTO {a}.TB_LOG_LAYOUT_EXTR\
                   (logId, layoutExtrID, nombLayout, fecIniGen_ts, estadoGen)\
                   VALUES (?, ?, ?, ?, ?)".format(a=gc.bdcfg))
        logging.info('Registrando Inicio de Extracción... ')
        #print(stm_sql)
        #print(lista)
        curtd.execute(stm_sql,lista)
        cntd.commit()
    except:
        logging.error('Ejecutando regStartExtrTD.')
        logging.error("%s", sys.exc_info()[0:])

def regEndExtrTD(curtd, lista):
    try:
        stm_sql = "UPDATE {a}.TB_LOG_LAYOUT_EXTR \
                   SET fecFinGen_ts = ?, estadoGen = ?,  tipoEjecucion = ? \
                   WHERE logId = ?".format(a=gc.bdcfg)

        logging.info('Registrando fin Extractor... ')
        curtd.execute(stm_sql,lista)
        cntd.commit()
    except:
        logging.error('Ejecutando regEndExtrTD.')
        logging.error("%s", sys.exc_info()[0:])



def regStartFtpTD(curtd, lista):
   try:
      sql = ("UPDATE {a}.TB_LOG_LAYOUT_EXTR\
             SET fecIniFtp_ts = ?, EstadoFtp = ?, archivoGenerado = ?\
             , pesoArchivo = ?, cant_intentos = coalesce(cant_intentos,0)+1\
             WHERE logId = ?".format(a=gc.bdcfg))
      logging.info('Registrando Inicio de regStartFtpTD... ')
      #print(sql)
      #print(lista)
      curtd.execute(sql,lista)
      cntd.commit()
   except:
      logging.error('Ejecutando regStartFtpTD.')
      logging.error("%s", sys.exc_info()[0:])

def regEndFtpTD(curtd, lista):
   try:
      stm_sql = "UPDATE {a}.TB_LOG_LAYOUT_EXTR \
                 SET fecFinFtp_ts = ?, estadoFtp = ? \
                 WHERE logId = ?".format(a=gc.bdcfg)

      logging.info('Registrando fin regEndFtpTD... ')
      curtd.execute(stm_sql,lista)
      cntd.commit()
   except:
      logging.error('Ejecutando regEndFtpTD.')
      logging.error("%s", sys.exc_info()[0:])

def regErrFtpTD(curtd, lista):
   try:
      stm_sql = "UPDATE {a}.TB_LOG_LAYOUT_EXTR \
                 SET fecFinFtp_ts = ?,estadoFtp =?, ftpDetError =?  \
                 WHERE logId = ?".format(a=gc.bdcfg)

      logging.info('Registrando regErrFtpTD... ')
      curtd.execute(stm_sql,lista)
      cntd.commit()
   except:
      logging.error('Ejecutando regErrFtpTD.')
      logging.error("%s", sys.exc_info()[0:])


if __name__ == "__main__":
    xlayout = xFechaIni = xFechaFin = None
    if len(sys.argv) >= 2:
        xlayout = sys.argv[1]
    if len(sys.argv) >= 3:
        xFechaIni = sys.argv[2]
    if len(sys.argv) >= 4:
        xFechaFin = sys.argv[3]

    run(xlayout,xFechaIni,xFechaFin);