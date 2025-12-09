import zipfile, versioning
from fns import SqlResources, Systempath, Processing
from pathlib import Path

if __name__ == '__main__':

    creditnotes = SqlResources()
    path = Systempath()

    # Obtiene listas con los elementos que se van a usar de 
    # los archivos zip y xml dentro de los zip.
    zip_files = path.filter_extension(path.files, ".zip")
    xml_files = path.filter_extension(path.files, ".xml") 

    # Aqui guarda la informacion de todos los xml leidos y
    # encontrados mas adelante.
    xml_to_sql = []

    # De los archivos zip encontrados, parsea uno a uno. 
    print("Inicia la lectura de los archivos.")
    for zip in zip_files:
        zip = Path(zip)
        validacion = path.modificado_recientemente(zip)
        if not validacion:
            continue

        print(f"Trabajando con: {zip.name}")

        #Lee el archivo zip.
        with zipfile.ZipFile(zip, "r") as zip:
            # Por cada archivo dentro del zip...
            for file in zip.namelist():
                # Filtra solo los archivos .xml y si lo encuentra...
                if file.lower().endswith(".xml"):
                    # Lee el archivo. 
                    with zip.open(file, "r") as xml_file:
                        # Aqui guarda la informacion del archivo xml
                        # que actualmente se está leyendo.
                        xml = xml_file.read().decode("utf-8")
                        # -> Busca los datos necesarios en el xml.
                        # -> Consulta datos en la DB. 
                        # Y con estos datos crea un diccionario.
                        datos_xml = Processing.parse_xml(xml, creditnotes.registred_nc)
                        # Valida que sea un diccionario.
                        if isinstance(datos_xml, dict):
                            # Agrega en una lista de diccionarios
                            # la informacion obtenida de parsear, formatear,
                            # y procesar la informacion del xml.
                            xml_to_sql.append(datos_xml)
                        else:
                            #print(f"Warning: El archivo {file} no generó datos válidos.")
                            pass


    if not xml_to_sql:
        print("No hay informacion para ingresar. ")
    else: 
        print("Insertando datos en la tabla.")
        SqlResources.insert_sql(xml_to_sql)
        print("Listo, datos insertados en Aux.")
        SqlResources.insert_sql_exec(xml_to_sql)
        print(f"Total de registros: {len(xml_to_sql)}")
    
    print(f"Script Finalizado.")



