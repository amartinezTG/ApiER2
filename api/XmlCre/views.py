# api/views/xmlCre.py
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import pyodbc
from datetime import date, timedelta
from api.db_connections import CONTROLGASTG_CONN_STR
from api.modelos.Volumetricos_Mensuales import VolumetricosMensuales
from api.modelos.estaciones_despachos import EstacionDespachos
from api.modelos.despachos_mensuales import DespachosMensuales
import xml.etree.ElementTree as ET
from collections import defaultdict
from api.XmlCre.utils.xml_parser import ESTACIONES
from concurrent.futures import ThreadPoolExecutor


def last_day_previous_month(today: date = None) -> date:
    """Obtiene el último día del mes anterior"""
    if today is None:
        today = date.today()
    first_this_month = today.replace(day=1)
    return first_this_month - timedelta(days=1)


def get_month_range(target_date: date) -> tuple:
    """
    Obtiene el primer y último día del mes de una fecha dada
    
    Args:
        target_date: Fecha del mes a procesar
        
    Returns:
        Tupla con (primer_dia, ultimo_dia)
    """
    primer_dia = target_date.replace(day=1)
    
    # Obtener último día del mes
    if target_date.month == 12:
        ultimo_dia = target_date.replace(day=31)
    else:
        siguiente_mes = target_date.replace(month=target_date.month + 1, day=1)
        ultimo_dia = siguiente_mes - timedelta(days=1)
    
    return primer_dia, ultimo_dia


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
                "MarcaComercial": text_or_none(prod.find(".//Covol:MarcaComercial", NS)),
                "Origen": "XML_mensual"
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
    
    if file_path:
        try:
            nombre_archivo = file_path.split('/')[-1] if '/' in file_path else file_path.split('\\')[-1]
        except:
            nombre_archivo = None
    
    if not nombre_archivo or not nombre_archivo.startswith("M_"):
        clave_instalacion = row.get('clave_instalacion')
        periodo = row.get('periodo')
        tipo_actividad = row.get('tipo_actividad')
        formato = row.get('formato')
        
        clave_str = str(clave_instalacion or "").strip()
        if clave_str and not clave_str.startswith("EDS-"):
            clave_str = f"EDS-{clave_str}"
        
        if isinstance(periodo, str):
            yymm = periodo[:7]
        else:
            yymm = periodo.strftime("%Y-%m")
        
        tipo = (tipo_actividad or "").strip()
        fmt = (formato or "").strip()
        nombre_archivo = f"M_{yymm}_{clave_str}_{tipo}_{fmt}.xml"
    
    return nombre_archivo


def procesar_xml_rows(xml_rows):
    """Procesa todas las filas de XML y extrae los productos"""
    all_productos = []
    
    for row in xml_rows:
        xml_contenido = row.get('xml_contenido')
        codigo_estacion = str(row.get('Codigo')).zfill(4) if row.get('Codigo') else None
        
        if not xml_contenido or not codigo_estacion:
            print(f"[WARNING] Fila sin XML o código de estación: ID {row.get('id')}")
            continue
        
        nombre_archivo = generar_nombre_archivo(row)
        print(f"[DEBUG] Procesando: {nombre_archivo}, Estación: {codigo_estacion}")
        
        productos = parse_volumetricos_formato_excel(xml_contenido, nombre_archivo, codigo_estacion)
        
        if productos:
            print(f"[DEBUG] Se encontraron {len(productos)} productos en el XML")
            all_productos.extend(productos)
        else:
            print(f"[WARNING] No se pudieron parsear productos del XML: {nombre_archivo}")
    
    return all_productos


def combinar_datos_xml_y_db(productos_xml, despachos_db):
    """
    Combina los datos del XML y la base de datos en un solo arreglo.
    Normaliza los nombres de campos para que sean consistentes.
    
    Args:
        productos_xml: Lista de productos desde XML
        despachos_db: Lista de productos desde base de datos
        
    Returns:
        Lista combinada de productos con estructura uniforme
    """
    resultado_combinado = []
    
    # Agregar productos del XML (ya tienen la estructura correcta)
    for producto in productos_xml:
        resultado_combinado.append(producto)
    
    # Agregar productos de la DB, normalizando los campos
    for despacho in despachos_db:
        # Crear estructura similar al XML pero con datos de DB
        producto_db = {
            "Producto": despacho.get("Producto"),
            "codprd": despacho.get("codprd"),
            "Origen": despacho.get("Origen", "DB_despachos"),
            
            # Métricas principales
            "TotalEntregasMes": despacho.get("TotalEntregasMes"),
            "SumaVolumenEntregadoMes_ValorNumerico": despacho.get("SumaVolumenEntregadoMes_ValorNumerico"),
            "monto": despacho.get("monto"),
            
            # Métricas de documentos
            "TotalDocumentosMes": despacho.get("TotalDocumentosMes"),
            "ImporteTotalEntregasMes": despacho.get("ImporteTotalEntregasMes"),
            "SumaVolumenCFDIs": despacho.get("SumaVolumenCFDIs"),
            "documentos": despacho.get("documentos"),
            
            # Campos que no tiene DB pero sí XML (los ponemos en None)
            "archivo": None,
            "Estación": None,
            "FechaYHoraReporteMes": None,
            "MarcaComercial": despacho.get("Producto")  # Usamos Producto como MarcaComercial
        }
        
        resultado_combinado.append(producto_db)
    
    print(f"[INFO] Combinados {len(productos_xml)} productos XML + {len(despachos_db)} productos DB = {len(resultado_combinado)} total")
    
    return resultado_combinado


@api_view(['GET', 'POST'])
def xmlCre(request):
    """
    Endpoint principal para procesar volumétricos mensuales y comparar con despachos
    """
    print("=" * 50)
    print("INICIANDO xmlCre")
    print("=" * 50)
    
    volumetricos_model = VolumetricosMensuales()
    estacion_despachos = EstacionDespachos()
    despachos_model = DespachosMensuales()

    try:
        # Obtener fecha del mes pasado
        target_period = last_day_previous_month()
        target_str = target_period.strftime("%Y-%m-%d")
        codgas = request.data.get('codgas', None)

        # Ejecutar consultas en paralelo
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_xml_rows = executor.submit(
                volumetricos_model.control_volumetricos_mensuales, 
                target_str, 
                codgas
            )
            future_estacion = executor.submit(
                estacion_despachos.estacion_by_id, 
                codgas
            )
            
            try:
                xml_rows = future_xml_rows.result(timeout=60)
                estacion = future_estacion.result(timeout=60)
            except Exception as e:
                print(f"[ERROR] Error al obtener resultados: {e}")
                return Response({
                    "success": False,
                    "error": f"Error al obtener resultados: {str(e)}"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not xml_rows:
            return Response({
                "success": False,
                "estacion": estacion,
                "periodo": target_str,
                "codgas": codgas,
                "data": [],
                "message": f"No se encontraron registros para el período {target_str}"
            }, status=status.HTTP_404_NOT_FOUND)

        print(f"[INFO] Se encontraron {len(xml_rows)} registros XML")

        # Procesar todos los XMLs
        all_productos = procesar_xml_rows(xml_rows)
        print(f"[INFO] Total de productos procesados: {len(all_productos)}")

        # Obtener datos de despachos si hay información de estación
        despachos_data = []
        if estacion and len(estacion) > 0:
            estacion_info = estacion[0]
            servidor = estacion_info.get('Servidor')
            base_datos = estacion_info.get('BaseDatos')

            if servidor and base_datos:
                print(f"[INFO] Consultando despachos en {servidor}/{base_datos}")

                # Obtener rango del mes
                fecha_inicial, fecha_final = get_month_range(target_period)
                print(f"[INFO] Periodo de consulta: {fecha_inicial} - {fecha_final}")
                
                try:
                    despachos_data = despachos_model.obtener_resumen_productos(
                        servidor=servidor,
                        base_datos=base_datos,
                        fecha_inicial=fecha_inicial,
                        fecha_final=fecha_final
                    )
                    print(f"[INFO] Se obtuvieron {len(despachos_data)} productos de despachos")
                except Exception as e:
                    print(f"[WARNING] Error al obtener despachos: {e}")
                    import traceback
                    traceback.print_exc()
                    despachos_data = []

        # NUEVO: Combinar ambas fuentes de datos
        datos_combinados = combinar_datos_xml_y_db(all_productos, despachos_data)

        return Response({
            "success": True,
            "periodo": target_str,
            # "estacion": estacion,
            # "total_registros_xml": len(xml_rows),
            # "total_productos_xml": len(all_productos),
            # "total_productos_db": len(despachos_data),
            # "total_combinado": len(datos_combinados),
            "datos": datos_combinados  # ARRAY ÚNICO COMBINADO
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