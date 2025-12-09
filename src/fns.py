import os, re, sqlalchemy, pyodbc, time
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Dict, Union
from dotenv import load_dotenv

load_dotenv()
CONN_STR = str(os.getenv('CONN_STR'))
BASE_PATH = Path(os.getenv('BASE_PATH'))
QUERY_NC = str(os.getenv('QUERY_NC'))
INSERT_VENDOR = str(os.getenv('INSERT_VENDOR'))
EXEC_STP_SAVE = str(os.getenv('EXEC_STP_SAVE'))
SELEC_INVOICE_DESTINATION = str(os.getenv('SELEC_INVOICE_DESTINATION'))

class Systempath():
    """
    ----------------------------------------------------------------
    Alojamiento de recursos relacionados al systema.
    """
    def __init__(self):
        """
        ----------------------------------------------------------------
        Con 'self.path_base' Define la ruta desde donde iniciara la busqueda
        recursiva de los documentos. 
        Con 'self.files' se obtiene las rutas de todos los archivos que se deben
        leer.
        """
        
        self.path_base = BASE_PATH
        self.files = self.path_files(self.path_base)

    def path_files(self, ruta_base: str) -> list:
        """
        Lee recursivamente todos los directorios y subdirectorios a partir de una ruta base,
        y guarda las rutas completas de los archivos encontrados en una lista.
        
        Args:
            ruta_base (str): La ruta base desde donde iniciar la búsqueda.
        
        Returns:
            list: Lista de rutas completas de los archivos encontrados.
        """
        _files = []
        for root, _, files in os.walk(ruta_base):
            for file in files:
                _files.append(os.path.join(root, file))
        return _files
    
    def filter_extension(self, path_list: list, ext: str) -> list:
        """
        Filra segun la extencion proporcionada una lista de archivos y devuelve
        una lista con los archivos que coincidan con la extencion. 
        Args:
            files (list): Lista de archivos a filtrar.
            ext (str): Extencion, como parametro para el filtro.
        Return: 
            Lista de rutas que coinciden con la extencion proporcionada.
        """
        returned_path = []
        for path in path_list:
            if str(path).lower().endswith(ext) == True:
                returned_path.append(path)
        return returned_path
    
    def obtener_fecha_modificacion(self, ruta_archivo: Path) -> datetime:
        """Obtiene la fecha de modificación de un archivo."""
        if not ruta_archivo.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {ruta_archivo}")
        
        if not ruta_archivo.is_file():
            raise ValueError(f"No es un archivo: {ruta_archivo}")
        
        timestamp = ruta_archivo.stat().st_mtime
        return datetime.fromtimestamp(timestamp)
    
    def fue_modificado(self, fecha_modificacion: datetime, tiempo: int = 30) -> bool:
        """Valida si una fecha está dentro de un rango de días."""
        hoy = datetime.now()
        tiempo_validar = hoy - timedelta(days=tiempo)
        
        # Asegurarnos de que ambas fechas estén en la misma zona horaria (sin tzinfo)
        fecha_mod_sin_tz = fecha_modificacion.replace(tzinfo=None)
        hoy_sin_tz = hoy.replace(tzinfo=None)
        tiempo_validar_sin_tz = tiempo_validar.replace(tzinfo=None)
        
        return tiempo_validar_sin_tz <= fecha_mod_sin_tz <= hoy_sin_tz
    
    def modificado_recientemente(self, ruta_archivo: Union[str, Path]) -> bool:
        """Verifica si un archivo fue modificado recientemente."""
        try: 
            if isinstance(ruta_archivo, str):
                ruta_archivo = Path(ruta_archivo)
            
            fecha_mod = self.obtener_fecha_modificacion(ruta_archivo)
            validacion = self.fue_modificado(fecha_mod)         
            return validacion
            
        except FileNotFoundError:
            print(f"-> ERROR: Archivo no encontrado: {ruta_archivo}")
            return False
        except ValueError as e:
            print(f"-> ERROR: {e}")
            return False
        except Exception as e:
            print(f"-> ERROR: Ocurrió un error al leer el archivo.")
            print(f"DETALLES:\n{type(e).__name__}: {e}")
            return False
        

class SqlResources():
    """
    ----------------------------------------------------------------
    Alojamiento de recursos para Sql y sus funciones.
    """
    def __init__(self):
        self.query_nc = QUERY_NC
        self.registred_nc = SqlResources.query_sql(self.query_nc)

    def query_sql(query):
        """
        Realiza consultas a SQL.
        """
        try:
            engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}",pool_pre_ping=True,fast_executemany=True)
            resultado = pd.read_sql(query, engine)
        except Exception as e:
            print("Error en la conexión o en la ejecución del query:\n", e)
            if engine:
                engine.dispose()
                
            print(f"Script Finalizado.")
            exit()
        else:
            return resultado
    

    def insert_sql(data: list[Dict[str, any]]):
        """
        Inserta datos en SQL. 
        """
        try:
            conn_str = CONN_STR
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    batch_size = 1000  # Máximo permitido por SQL Server
                    for i in range(0, len(data), batch_size):
                        batch =data[i:i + batch_size]
                        values = ", ".join([
                            f"({d['vendorid']}, {d['stationid']}, '{d['fecha']}', '{d['description']}', '{d['invoice']}', '{d['rel']}', "
                            f"'{d['creditnotenumber']}', '{d['tad']}', {float(d['importe'])}, {float(d['total'])}, {d['destinationid']}, '{d['uuid']}')"
                            for d in batch
                        ])
                        query = f"""{INSERT_VENDOR} {values}"""

                    cursor.execute(query)
                    conn.commit()
                    msg = "Datos ingresados correctamente en Aux"
                    print(msg)
                    return msg
                
        except pyodbc.Error as e:
            print("Error en la conexión o en la ejecución del query:", e)
        except Exception as e:
            print("Ocurrió un error inesperado:", e)

    def insert_sql_exec(data: list[Dict[str, any]]):
        """
        Inserta datos en SQL ejecutando un procedimiento almacenado.
        """
        try:
            conn_str = CONN_STR
            with pyodbc.connect(conn_str) as conn:
                with conn.cursor() as cursor:
                    for d in data:
                        print(f"Ejecutando EXEC con los datos:\n {d}")
                        time.sleep(3)

                        query = f"""
                            {EXEC_STP_SAVE}
                                @vendorId = {d['vendorid']},
                                @stationId = {d['stationid']},
                                @date = '{d['fecha']}',
                                @productName = '{d['description']}',
                                @remision = '{d['rel']}',
                                @invoice = '{d['invoice']}',
                                @creditNoteNumber = '{d['creditnotenumber']}',
                                @tarTad = '{d['tad']}',
                                @tax = {float(d['importe'])},
                                @total = {float(d['total'])},
                                @destinationName = '{d['destinationid']}',
                                @FiscalFolio = '{d['uuid']}';
                            """
                        print(f"Ejecutando EXEC")
                        print(query)
                        cursor.execute(query)
                        print(f"Listo.")
                        conn.commit()
                    msg = "Datos ingresados correctamente."
                    print(msg)
                    return msg
                    
        except pyodbc.Error as e:
            print("Error en la conexión o en la ejecución del query:", e)
        except Exception as e:
            print("Ocurrió un error inesperado al ejecutar el EXEC:", e)

class Processing():
    """
    ----------------------------------------------------------------
    Alojamiento de funciones realcionadas a el procesamiento de documentos
    """

    def parse_xml(xmlstring: str, df: pd.DataFrame) -> Dict[str, any]:
        """
        ----------------------------------------------------------------
        Valida la informacion de un .XML y devuelve un diccionario con
        ciertos valores.

        Del xml obtendra: 
        
        Tipo de comprobante, Fecha, Total, Serie, Folio, 
        Numero de nota de credito (serie + folio), Conceptos,
        Descripcion, Remision, Tad, Relacion, Traslado,
        Importe del traslado, Timbre fiscal, UUID, 
        Destinationid (De la DB.), Invoice (De la DB.)
        """

        try:
            root = ET.fromstring(xmlstring)
            # Definir namespace
            namespaces = {
                "cfdi": "http://www.sat.gob.mx/cfd/4",
                "tfd": "http://www.sat.gob.mx/TimbreFiscalDigital",
                "pm": "http://pemex.com/facturaelectronica/addenda/v2"
            }
            # Valores
            tipodecomprobante = root.attrib.get("TipoDeComprobante")
            fecha = root.attrib.get("Fecha")[:10]
            total = root.attrib.get("Total")
            creditnotenumber = root.attrib.get("Serie") + "-" + root.attrib.get("Folio")

            # Evita procesar una nota de credito en especial.
            # Ya no es problema, pero podemos agregar una lista
            # De 'creditnotenumber' a omitir.
            """if '3512872' in creditnotenumber:
                input("ALTO")
                return None"""

            # Salta los comprobantes tipo 'E'.
            if tipodecomprobante != "E":
                #print("El 'TipoDeComprobante' es diferente de 'E'")
                return None
            
            # Salta los 'creditnotenumber' ya registrados.
            if creditnotenumber in df['CreditNoteNumber'].values:
                #print(f"El creditnotenumber '{creditnotenumber}' ya existe en el DataFrame.")
                return None
            else:
                print(f"El NC: {creditnotenumber} NO SE HA REGISTRADO.")

            # print(creditnotenumber)

            conceptos = root.find("cfdi:Conceptos", namespaces)
            if conceptos is not None:
                for concepto in conceptos.findall("cfdi:Concepto", namespaces):
                    description = concepto.get("Descripcion")
            else:
                print("No se encontró el nodo cfdi:Conceptos")
                return
            
            remision = root.find(".//pm:NREMISION", namespaces)
            if remision is not None:
                #rem = re.search(r"(\d+)$", remision.text.replace("  ", "")).group(1)
                tad = re.search(r"RC-(\d+)", remision.text.replace("  ", "")).group(1)
            else:
                print("No se encontró el nodo pm:NREMISION")
                return

            relacion = root.find(".//pm:A_RELACION", namespaces)
            if relacion is not None:
                rel = re.search(r"(\d+)$", relacion.text.replace("  ", "")).group(1)
            else:
                print("No se encontró el nodo pm:A_RELACION")
                return

            traslado = root.find(".//cfdi:Traslado", namespaces)
            if traslado is not None:
                importe = traslado.attrib.get("Importe")
            else:
                print("No se encontró el nodo .//cfdi:Traslado")
                return

            timbre = root.find(".//tfd:TimbreFiscalDigital", namespaces)
            if timbre is not None:
                uuid = timbre.attrib.get("UUID")
            else:
                print("No se encontró el nodo .//tfd:TimbreFiscalDigital")
                return

            _query = f"""{SELEC_INVOICE_DESTINATION} remision = '{rel}' and tar_tad = '{tad}'"""
            
            # Imprime el query. (¿Despreciar?)
            print(_query)
            
            df = SqlResources.query_sql(_query)
            if df.empty:
                print(f"La Remisión: {rel} no devolvió un 'DestinationId' válido.")
                return
            
            destinationid = df['ID'].values[0]
            invoice = df['Invoice'].values[0]

            # Crea un diccionario con los datos obtenidos del xml y la DB.
            dc = {
                "vendorid": 1,  # Reemplaza con el valor correspondiente si aplica
                "stationid": 44,  # Reemplaza con el valor correspondiente si aplica
                "fecha": fecha,
                "description": description,
                "rel": rel,
                "invoice": invoice,
                "creditnotenumber": creditnotenumber,
                "tad": tad,
                "importe": importe,
                "total": total,
                "destinationid": destinationid,  # Reemplaza con el valor correspondiente si aplica
                "uuid": uuid
            }
            #print(f"Los siguientes datos fueron encontrados en el xml:")
            #print(f"{dc}")
            return(dc)

        except ET.ParseError:
            print("Error al analizar el XML. Verifica el formato del archivo.")
            return 
        
