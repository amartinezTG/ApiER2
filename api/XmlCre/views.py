# api/views/xmlCre.py (actualizado)
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import pyodbc
from datetime import date, timedelta
from api.db_connections import CONTROLGASTG_CONN_STR
from api.modelos.Volumetricos_Mensuales import VolumetricosMensuales
from api.modelos.Volumetricos_Diarios import VolumetricosDiarios
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


def combinar_datos_xml_y_db(productos_xml, despachos_db, volumetricos_diarios_data):
    """
    Combina los datos del XML mensual, base de datos y volumétricos diarios
    
    Args:
        productos_xml: Lista de productos desde XML mensual
        despachos_db: Lista de productos desde base de datos
        volumetricos_diarios_data: Datos consolidados de XMLs diarios
        
    Returns:
        Lista combinada de productos con estructura uniforme
    """
    resultado_combinado = []
    
    # Agregar productos del XML mensual
    for producto in productos_xml:
        resultado_combinado.append(producto)
    
    # Agregar productos de la DB
    for despacho in despachos_db:
        producto_db = {
            "Producto": despacho.get("Producto"),
            "codprd": despacho.get("codprd"),
            "Origen": despacho.get("Origen", "DB_despachos"),
            "TotalEntregasMes": despacho.get("TotalEntregasMes"),
            "SumaVolumenEntregadoMes_ValorNumerico": despacho.get("SumaVolumenEntregadoMes_ValorNumerico"),
            "monto": despacho.get("monto"),
            "TotalDocumentosMes": despacho.get("TotalDocumentosMes"),
            "ImporteTotalEntregasMes": despacho.get("ImporteTotalEntregasMes"),
            "SumaVolumenCFDIs": despacho.get("SumaVolumenCFDIs"),
            "documentos": despacho.get("documentos"),
            "archivo": None,
            "Estación": None,
            "FechaYHoraReporteMes": None,
            "MarcaComercial": despacho.get("Producto")
        }
        resultado_combinado.append(producto_db)
    
    # Agregar productos de volumétricos diarios
    if volumetricos_diarios_data and volumetricos_diarios_data.get('success'):
        for producto_diario in volumetricos_diarios_data.get('datos', []):
            producto_consolidado = {
                "ClaveProducto": producto_diario.get("ClaveProducto"),
                "ClaveSubProducto": producto_diario.get("ClaveSubProducto"),
                "VolumenTotalMes": producto_diario.get("VolumenTotalMes"),
                "ImporteTotalMes": producto_diario.get("ImporteTotalMes"),
                "TotalTransaccionesMes": producto_diario.get("TotalTransaccionesMes"),
                "DiasConVenta": producto_diario.get("DiasConVenta"),
                "Origen": producto_diario.get("Origen"),
                "DetalleDiario": producto_diario.get("DetalleDiario")
            }
            resultado_combinado.append(producto_consolidado)
    
    print(f"[INFO] Combinados: {len(productos_xml)} XML + {len(despachos_db)} DB + {len(volumetricos_diarios_data.get('datos', []) if volumetricos_diarios_data else [])} Diarios = {len(resultado_combinado)} total")
    
    return resultado_combinado


@api_view(['GET', 'POST'])
def xmlCre(request):
    """
    Endpoint principal para procesar volumétricos mensuales, diarios y comparar con despachos
    """
    print("=" * 50)
    print("INICIANDO xmlCre")
    print("=" * 50)

    volumetricos_model = VolumetricosMensuales()
    volumetricos_diarios_model = VolumetricosDiarios(
        usuario="Administrador",
        password="T0t4lG4s2020"
    )
    estacion_despachos = EstacionDespachos()
    despachos_model = DespachosMensuales()

    try:
        # Obtener fecha del mes pasado
        target_period = last_day_previous_month()
        target_str = target_period.strftime("%Y-%m-%d")
        codgas = request.data.get('codgas', None)
        estacion =  estacion_despachos.estacion_by_id(codgas) if codgas else None

        # Ejecutar consultas en paralelo
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_xml_rows = executor.submit(
                volumetricos_model.control_volumetricos_mensuales, 
                target_str, 
                codgas
            )

            try:
                xml_rows = future_xml_rows.result(timeout=60)
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

        # Procesar XMLs mensuales
        all_productos = []
        all_productos = procesar_xml_rows(xml_rows) ###descomentar para producción

        # Obtener datos de despachos
        despachos_data = []
        if estacion and len(estacion) > 0:
            estacion_info = estacion[0]
            servidor = estacion_info.get('Servidor')
            base_datos = estacion_info.get('BaseDatos')

            if servidor and base_datos:
                fecha_inicial, fecha_final = get_month_range(target_period)
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

        # NUEVO: Procesar volumétricos diarios usando PermisoCRE
        volumetricos_diarios_data = None

        if estacion and len(estacion) > 0:
            estacion_info = estacion[0]
            permiso_cre = estacion_info.get('PermisoCRE')  # CAMBIO: Usar PermisoCRE en lugar de Servidor

            if permiso_cre:
                print(f"[INFO] Procesando volumétricos diarios del permiso {permiso_cre}")
                try:
                    volumetricos_diarios_data = volumetricos_diarios_model.generar_resumen_mensual(
                        permiso_cre=permiso_cre,  # CAMBIO: Pasar permiso_cre en lugar de ip_servidor
                        mes=target_period,
                        desconectar_al_final=True
                    )
                    if volumetricos_diarios_data.get('success'):
                        print(f"[INFO] Se procesaron {volumetricos_diarios_data.get('archivos_procesados')} archivos diarios")
                    else:
                        print(f"[WARNING] No se pudieron procesar volumétricos diarios: {volumetricos_diarios_data.get('mensaje')}")
                except Exception as e:
                    print(f"[WARNING] Error al procesar volumétricos diarios: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"[WARNING] No se encontró PermisoCRE para la estación {codgas}")


        # Combinar todas las fuentes de datos
        datos_combinados = combinar_datos_xml_y_db(all_productos, despachos_data, volumetricos_diarios_data)

        return Response({
            "success": True,
            "periodo": target_str,
            "volumetricos_diarios": volumetricos_diarios_data,
            "datos": datos_combinados
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