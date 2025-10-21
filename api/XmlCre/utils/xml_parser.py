import xml.etree.ElementTree as ET
import re

# =========================
#  NAMESPACES y MAPEOS
# =========================
NS = {
    "Covol": "https://repositorio.cloudb.sat.gob.mx/Covol/xml/Mensuales",
    "exp": "Complemento_Expendio",
}

ESTACIONES = {
    "0002": "Gemela Grande",
    # "0003": "Aguascalientes",
    "0005": "Lerdo",
    # "0006": "Lopez Mateos",
    # "0007": "Gemela Chica",
    # "0008": "Municipio Libre",
    # "0009": "Aztecas",
    # "0010": "Misiones",
    # "0011": "Puerto de palos",
    # "0012": "Miguel de la madrid",
    # "0013": "Permuta",
    # "0014": "Electrolux",
    # "0015": "Aeronáutica",
    # "0016": "Custodia",
    # "0017": "Anapra",
    # "0018": "Parral",
    # "0019": "Delicias",
    # "0021": "Plutarco",
    # "0022": "Tecnológico",
    # "0023": "Ejército Nacional",
    # "0024": "Satélite",
    # "0025": "Las fuentes",
    # "0026": "Clara",
    # "0027": "Solis",
    # "0028": "Santiago Troncoso",
    # "0029": "Jarudo",
    # "0030": "Hermanos Escobar",
    # "0031": "Villa Ahumada",
    # "0032": "El castaño",
    # "0033": "Travel Center",
    # "0034": "Picachos",
    # "0035": "Ventanas",
    # "0036": "San Rafael",
    # "0037": "Puertecito",
    # "0038": "Jesus Maria",
    # "0039": "Gabriela Mistral",
    # "0040": "PRAXEDIS",
}

FIELDS = {
    "ClaveProducto": ".//Covol:ClaveProducto",
    "ClaveSubProducto": ".//Covol:ClaveSubProducto",
    "ComposOctanajeGasolina": ".//Covol:Gasolina/Covol:ComposOctanajeGasolina",
    "GasolinaConCombustibleNoFosil": ".//Covol:Gasolina/Covol:GasolinaConCombustibleNoFosil",
    "MarcaComercial": ".//Covol:MarcaComercial",
}

ENTREGAS_FIELDS = {
    "TotalEntregasMes": "Covol:TotalEntregasMes",
    "SumaVolumenEntregadoMes_ValorNumerico": "Covol:SumaVolumenEntregadoMes/Covol:ValorNumerico",
    "SumaVolumenEntregadoMes_UM": "Covol:SumaVolumenEntregadoMes/Covol:UM",
    "TotalDocumentosMes": "Covol:TotalDocumentosMes",
    "ImporteTotalEntregasMes": "Covol:ImporteTotalEntregasMes",
}

def text_or_none(elem):
    """Extrae texto de un elemento XML o retorna None"""
    return elem.text.strip() if elem is not None and elem.text is not None else None

def parse_xml_string(xml_string: str, file_name: str = ""):
    """
    Parsea un string XML y extrae la información de volumétricos
    
    Args:
        xml_string: Contenido XML como string
        file_name: Nombre del archivo (para extraer código de estación)
    
    Returns:
        Lista de diccionarios con los datos extraídos
    """
    rows = []
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"[WARN] No se pudo parsear XML: {e}")
        return rows

    fecha_reporte = text_or_none(root.find(".//Covol:FechaYHoraReporteMes", NS))

    # Extraer código de estación del nombre del archivo
    match = re.search(r"EDS-(\d+)", file_name)
    cod_estacion = match.group(1) if match else None
    nombre_estacion = ESTACIONES.get(
        cod_estacion, 
        f"Desconocida ({cod_estacion})" if cod_estacion else "No encontrado"
    )

    # Iterar sobre cada producto
    for prod in root.findall(".//Covol:PRODUCTO", NS):
        row = {
            "archivo": file_name,
            "Estacion": nombre_estacion,
            "CodigoEstacion": cod_estacion,
            "FechaYHoraReporteMes": fecha_reporte,
        }
        
        # Extraer campos básicos del producto
        for col, xpath in FIELDS.items():
            row[col] = text_or_none(prod.find(xpath, NS))

        # Extraer información de entregas
        entregas = prod.find(".//Covol:REPORTEDEVOLUMENMENSUAL/Covol:ENTREGAS", NS)
        if entregas is not None:
            for col, rel_xpath in ENTREGAS_FIELDS.items():
                row[col] = text_or_none(entregas.find(rel_xpath, NS))
            
            # Calcular suma de volúmenes de CFDIs
            suma_cfdis = 0.0
            for comp in entregas.findall(".//Covol:Complemento/Covol:Complemento_Expendio", NS):
                for val in comp.findall(".//exp:VolumenDocumentado/exp:ValorNumerico", NS):
                    try:
                        suma_cfdis += float(val.text.strip())
                    except Exception:
                        pass
            row["SumaVolumenCFDIs"] = suma_cfdis
        else:
            for col in ENTREGAS_FIELDS.keys():
                row[col] = None
            row["SumaVolumenCFDIs"] = None

        rows.append(row)
    
    return rows