from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import pyodbc
from datetime import date, timedelta
from api.db_connections import CONTROLGASTG_CONN_STR
from api.modelos.Volumetricos_Mensuales import VolumetricosMensuales
import xml.etree.ElementTree as ET
import re
from collections import defaultdict
from api.XmlCre.utils.xml_parser import ESTACIONES

# Diccionario de estaciones


def last_day_previous_month(today: date = None) -> date:
    """Obtiene el último día del mes anterior"""
    if today is None:
        today = date.today()
    first_this_month = today.replace(day=1)
    return first_this_month - timedelta(days=1)

def text_or_none(elem):
    """Helper para extraer texto de elementos XML"""
    return elem.text.strip() if elem is not None and elem.text is not None else None

def parse_volumetricos_formato_excel(xml_string, nombre_archivo, cod_estacion):
    """Parsea el XML en el mismo formato que tu script de Excel"""
    try:
        NS = {
            "Covol": "https://repositorio.cloudb.sat.gob.mx/Covol/xml/Mensuales",
            "exp": "Complemento_Expendio"
        }
        
        root = ET.fromstring(xml_string)
        fecha_reporte = text_or_none(root.find(".//Covol:FechaYHoraReporteMes", NS))
        nombre_estacion = ESTACIONES.get(cod_estacion, f"Desconocida ({cod_estacion})")
        
        productos_data = []
        
        for prod in root.findall(".//Covol:PRODUCTO", NS):
            row = {
                "archivo": nombre_archivo,
                "Estación": nombre_estacion,
                "FechaYHoraReporteMes": fecha_reporte,
                "MarcaComercial": text_or_none(prod.find(".//Covol:MarcaComercial", NS))
            }
            
            entregas = prod.find(".//Covol:REPORTEDEVOLUMENMENSUAL/Covol:ENTREGAS", NS)
            
            if entregas is not None:
                row["TotalEntregasMes"] = text_or_none(entregas.find("Covol:TotalEntregasMes", NS))
                row["SumaVolumenEntregadoMes_ValorNumerico"] = text_or_none(
                    entregas.find("Covol:SumaVolumenEntregadoMes/Covol:ValorNumerico", NS)
                )
                row["TotalDocumentosMes"] = text_or_none(entregas.find("Covol:TotalDocumentosMes", NS))
                row["ImporteTotalEntregasMes"] = text_or_none(entregas.find("Covol:ImporteTotalEntregasMes", NS))
                
                suma_cfdis = 0.0
                for comp in entregas.findall(".//Covol:Complemento/Covol:Complemento_Expendio", NS):
                    for val in comp.findall(".//exp:VolumenDocumentado/exp:ValorNumerico", NS):
                        try:
                            suma_cfdis += float(val.text.strip())
                        except Exception:
                            pass
                
                row["SumaVolumenCFDIs"] = suma_cfdis
            else:
                row["TotalEntregasMes"] = None
                row["SumaVolumenEntregadoMes_ValorNumerico"] = None
                row["TotalDocumentosMes"] = None
                row["ImporteTotalEntregasMes"] = None
                row["SumaVolumenCFDIs"] = None
            
            # Convertir valores numéricos
            for campo in ["TotalEntregasMes", "SumaVolumenEntregadoMes_ValorNumerico", 
                         "TotalDocumentosMes", "ImporteTotalEntregasMes", "SumaVolumenCFDIs"]:
                if row[campo] is not None:
                    try:
                        if campo in ["SumaVolumenEntregadoMes_ValorNumerico", "ImporteTotalEntregasMes", "SumaVolumenCFDIs"]:
                            row[campo] = float(row[campo])
                        else:
                            row[campo] = int(row[campo])
                    except:
                        row[campo] = None
            
            productos_data.append(row)
        
        return productos_data
    
    except Exception as e:
        print(f"[ERROR] Error parseando XML: {str(e)}")
        return None


def generar_nombre_archivo(row):
    """Genera el nombre del archivo XML basado en los datos de la fila"""
    file_path = row.get('file_path')
    nombre_archivo = None
    
    # Intentar extraer nombre del file_path
    if file_path:
        try:
            nombre_archivo = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
        except:
            nombre_archivo = None
    
    # Si no hay nombre o no empieza con "M_", construirlo
    if not nombre_archivo or not nombre_archivo.startswith("M_"):
        clave_instalacion = row.get('clave_instalacion')
        periodo = row.get('periodo')
        tipo_actividad = row.get('tipo_actividad')
        formato = row.get('formato')
        
        clave_str = str(clave_instalacion or "").strip()
        if clave_str and not clave_str.startswith("EDS-"):
            clave_str = f"EDS-{clave_str}"
        
        # Manejar periodo como string o date
        if isinstance(periodo, str):
            yymm = periodo[:7]
        else:
            yymm = periodo.strftime("%Y-%m")
        
        tipo = (tipo_actividad or "").strip()
        fmt = (formato or "").strip()
        nombre_archivo = f"M_{yymm}_{clave_str}_{tipo}_{fmt}.xml"
    
    return nombre_archivo


def procesar_xml_rows(xml_rows):
    """
    Procesa todas las filas de XML y extrae los productos
    
    Args:
        xml_rows: Lista de diccionarios con datos de volumétricos
        
    Returns:
        Lista de productos extraídos de todos los XMLs
    """
    all_productos = []
    
    for row in xml_rows:
        xml_contenido = row.get('xml_contenido')
        codigo_estacion = str(row.get('Codigo')).zfill(4) if row.get('Codigo') else None
        
        if not xml_contenido or not codigo_estacion:
            print(f"[WARNING] Fila sin XML o código de estación: ID {row.get('id')}")
            continue
        
        # Generar nombre de archivo
        nombre_archivo = generar_nombre_archivo(row)
        
        print(f"[DEBUG] Procesando: {nombre_archivo}, Estación: {codigo_estacion}")
        
        # Parsear el XML
        productos = parse_volumetricos_formato_excel(xml_contenido, nombre_archivo, codigo_estacion)
        
        if productos:
            print(f"[DEBUG] Se encontraron {len(productos)} productos en el XML")
            all_productos.extend(productos)
        else:
            print(f"[WARNING] No se pudieron parsear productos del XML: {nombre_archivo}")
    
    return all_productos


def generar_resumen_por_producto(productos):
    """
    Genera un resumen agrupado por producto con totales
    
    Args:
        productos: Lista de productos procesados
        
    Returns:
        Lista de diccionarios con resumen por producto
    """
    # Usar defaultdict para evitar inicialización manual
    resumen = defaultdict(lambda: {
        "producto": "",
        "total_entregas": 0,
        "volumen_total": 0.0,
        "total_documentos": 0,
        "importe_total": 0.0,
        "suma_volumen_cfdis": 0.0,
        "registros": []
    })
    
    for prod in productos:
        marca = prod.get("MarcaComercial", "Sin marca")
        
        # Inicializar el nombre del producto si es la primera vez
        if not resumen[marca]["producto"]:
            resumen[marca]["producto"] = marca
        
        # Acumular totales (manejo seguro de None)
        if prod.get("TotalEntregasMes") is not None:
            resumen[marca]["total_entregas"] += prod["TotalEntregasMes"]
        
        if prod.get("SumaVolumenEntregadoMes_ValorNumerico") is not None:
            resumen[marca]["volumen_total"] += prod["SumaVolumenEntregadoMes_ValorNumerico"]
        
        if prod.get("TotalDocumentosMes") is not None:
            resumen[marca]["total_documentos"] += prod["TotalDocumentosMes"]
        
        if prod.get("ImporteTotalEntregasMes") is not None:
            resumen[marca]["importe_total"] += prod["ImporteTotalEntregasMes"]
        
        if prod.get("SumaVolumenCFDIs") is not None:
            resumen[marca]["suma_volumen_cfdis"] += prod["SumaVolumenCFDIs"]
        
        resumen[marca]["registros"].append(prod)
    
    # Convertir a lista y redondear valores
    resumen_lista = []
    for marca, datos in resumen.items():
        resumen_lista.append({
            "producto": datos["producto"],
            "total_entregas": datos["total_entregas"],
            "volumen_total": round(datos["volumen_total"], 3),
            "total_documentos": datos["total_documentos"],
            "importe_total": round(datos["importe_total"], 2),
            "suma_volumen_cfdis": round(datos["suma_volumen_cfdis"], 3),
            "num_estaciones": len(set(r["Estación"] for r in datos["registros"]))
        })
    
    return resumen_lista


@api_view(['GET', 'POST'])
def xmlCre(request):
    """
    Endpoint principal para procesar volumétricos mensuales
    """
    print("=" * 50)
    print("INICIANDO xmlCre")
    print("=" * 50)
    
    volumetricos_model = VolumetricosMensuales()

    try:
        # Obtener fecha del mes pasado
        target_period = last_day_previous_month()
        target_str = target_period.strftime("%Y-%m-%d")

        print(f"[INFO] Fecha objetivo para consulta: {target_str}")

        # Parámetros opcionales del request
        codgas = request.data.get('codgas', None)
        
        # Consultar base de datos
        xml_rows = volumetricos_model.control_volumetricos_mensuales(target_str, codgas)

        if not xml_rows:
            return Response({
                "success": False,
                "data": [],
                "message": f"No se encontraron registros para el período {target_str}"
            }, status=status.HTTP_404_NOT_FOUND)

        print(f"[INFO] Se encontraron {len(xml_rows)} registros XML")

        # Procesar todos los XMLs
        all_productos = procesar_xml_rows(xml_rows)
        
        print(f"[INFO] Total de productos procesados: {len(all_productos)}")

        # Generar resumen por producto
        # resumen_lista = generar_resumen_por_producto(all_productos)

        return Response({
            "success": True,
            "periodo": target_str,
            "total_registros_xml": len(xml_rows),
            "total_productos_procesados": len(all_productos),
            # "resumen_por_producto": resumen_lista,
            "detalle_completo": all_productos
        }, status=status.HTTP_200_OK)
            
    except pyodbc.Error as e:
        print(f"[ERROR] Error de base de datos: {e}")
        return Response({
            "success": False,
            "error": f"Error de base de datos: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    except Exception as e:
        print(f"[ERROR] Ocurrió un error: {e}")
        import traceback
        traceback.print_exc()
        return Response({
            "success": False,
            "error": str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)