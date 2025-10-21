# api/modelos/Volumetricos_Diarios.py
import os
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import List, Dict, Optional
from collections import defaultdict

class VolumetricosDiarios:
    """Procesa archivos XML de controles volumétricos diarios desde directorios de red"""
    
    def __init__(self):
        self.NS = {
            "cv": "http://www.sat.gob.mx/esquemas/controlesvolumetricos"
        }
    
    def construir_path_red(self, ip_servidor: str, mes: date) -> str:
        """
        Construye la ruta de red para acceder a los XML diarios
        Args:
            ip_servidor: IP del servidor (ej: "192.168.2.101")
            mes: Fecha del mes a consultar
        Returns:
            Ruta de red completa
        """
        # Formato: \\192.168.2.101\ControlVolumetrico\2025\09
        year = mes.strftime("%Y")
        month = mes.strftime("%m")
        return f"\\\\{ip_servidor}\\ControlVolumetrico\\{year}\\{month}"
    
    def obtener_archivos_xml_mes(self, ip_servidor: str, mes: date) -> List[str]:
        """
        Obtiene lista de archivos XML del mes especificado
        Args:
            ip_servidor: IP del servidor
            mes: Fecha del mes
        Returns:
            Lista de rutas completas a archivos XML
        """
        path = self.construir_path_red(ip_servidor, mes)
        
        try:
            if not os.path.exists(path):
                print(f"[WARNING] No existe el directorio: {path}")
                return []
            
            archivos = []
            for archivo in os.listdir(path):
                if archivo.endswith('.xml'):
                    ruta_completa = os.path.join(path, archivo)
                    archivos.append(ruta_completa)
            
            print(f"[INFO] Encontrados {len(archivos)} archivos XML en {path}")
            return sorted(archivos)
            
        except PermissionError:
            print(f"[ERROR] Sin permisos para acceder a {path}")
            return []
        except Exception as e:
            print(f"[ERROR] Error al listar archivos en {path}: {e}")
            return []
    
    def parsear_xml_diario(self, ruta_archivo: str) -> Optional[Dict]:
        """
        Parsea un archivo XML diario y extrae información de ventas
        Args:
            ruta_archivo: Ruta completa al archivo XML
        Returns:
            Diccionario con datos parseados o None si hay error
        """
        try:
            tree = ET.parse(ruta_archivo)
            root = tree.getroot()
            
            # Extraer datos básicos del control
            rfc = root.get('rfc')
            fecha_corte = root.get('fechaYHoraCorte')
            numero_permiso = root.get('numeroPermisoCRE')
            
            # Procesar ventas por producto
            ventas_por_producto = defaultdict(lambda: {
                'volumen_total': 0.0,
                'importe_total': 0.0,
                'num_transacciones': 0,
                'transacciones': []
            })
            
            # Buscar secciones VTA
            for vta in root.findall('.//cv:VTA', self.NS):
                # Procesar VTACabecera
                for cabecera in vta.findall('.//cv:VTACabecera', self.NS):
                    clave_producto = cabecera.get('claveProducto')
                    clave_subproducto = cabecera.get('claveSubProducto')
                    key = f"{clave_producto}-{clave_subproducto}"
                    
                    volumen = float(cabecera.get('sumatoriaVolumenDespachado', 0))
                    ventas = float(cabecera.get('sumatoriaVentas', 0))
                    
                    ventas_por_producto[key]['volumen_total'] += volumen
                    ventas_por_producto[key]['importe_total'] += ventas
                
                # Procesar VTADetalle
                for detalle in vta.findall('.//cv:VTADetalle', self.NS):
                    clave_producto = detalle.get('claveProducto')
                    clave_subproducto = detalle.get('claveSubProducto')
                    key = f"{clave_producto}-{clave_subproducto}"
                    
                    volumen = float(detalle.get('volumenDespachado', 0))
                    importe = float(detalle.get('importeTotalTransaccion', 0))
                    
                    if volumen > 0:  # Solo contar transacciones reales
                        ventas_por_producto[key]['num_transacciones'] += 1
                        ventas_por_producto[key]['transacciones'].append({
                            'volumen': volumen,
                            'importe': importe,
                            'fecha': detalle.get('fechaYHoraTransaccionVenta'),
                            'dispensario': detalle.get('numeroDispensario'),
                            'manguera': detalle.get('identificadorManguera')
                        })
            
            return {
                'archivo': os.path.basename(ruta_archivo),
                'rfc': rfc,
                'fecha_corte': fecha_corte,
                'numero_permiso': numero_permiso,
                'ventas': dict(ventas_por_producto)
            }
            
        except ET.ParseError as e:
            print(f"[ERROR] Error parseando XML {ruta_archivo}: {e}")
            return None
        except Exception as e:
            print(f"[ERROR] Error procesando {ruta_archivo}: {e}")
            return None
    
    def generar_resumen_mensual(self, ip_servidor: str, mes: date) -> Dict:
        """
        Genera resumen mensual consolidado de todos los XMLs diarios
        Args:
            ip_servidor: IP del servidor
            mes: Mes a procesar
        Returns:
            Diccionario con resumen mensual
        """
        archivos_xml = self.obtener_archivos_xml_mes(ip_servidor, mes)
        
        if not archivos_xml:
            return {
                'success': False,
                'mensaje': 'No se encontraron archivos XML',
                'datos': []
            }
        
        # Consolidar datos de todos los días
        consolidado = defaultdict(lambda: {
            'volumen_total_mes': 0.0,
            'importe_total_mes': 0.0,
            'total_transacciones_mes': 0,
            'dias_con_venta': set(),
            'detalle_diario': []
        })
        
        archivos_procesados = 0
        archivos_error = 0
        
        for archivo in archivos_xml:
            datos_dia = self.parsear_xml_diario(archivo)
            
            if datos_dia:
                archivos_procesados += 1
                for producto, ventas in datos_dia['ventas'].items():
                    consolidado[producto]['volumen_total_mes'] += ventas['volumen_total']
                    consolidado[producto]['importe_total_mes'] += ventas['importe_total']
                    consolidado[producto]['total_transacciones_mes'] += ventas['num_transacciones']
                    consolidado[producto]['dias_con_venta'].add(datos_dia['fecha_corte'][:10])
                    consolidado[producto]['detalle_diario'].append({
                        'archivo': datos_dia['archivo'],
                        'fecha': datos_dia['fecha_corte'],
                        'volumen': ventas['volumen_total'],
                        'importe': ventas['importe_total'],
                        'transacciones': ventas['num_transacciones']
                    })
            else:
                archivos_error += 1
        
        # Convertir a formato final
        resultado = []
        for producto, datos in consolidado.items():
            clave_prod, clave_subprod = producto.split('-')
            resultado.append({
                'ClaveProducto': clave_prod,
                'ClaveSubProducto': clave_subprod,
                'VolumenTotalMes': round(datos['volumen_total_mes'], 3),
                'ImporteTotalMes': round(datos['importe_total_mes'], 2),
                'TotalTransaccionesMes': datos['total_transacciones_mes'],
                'DiasConVenta': len(datos['dias_con_venta']),
                'DetalleDiario': datos['detalle_diario'],
                'Origen': 'XML_diarios_consolidado'
            })
        
        return {
            'success': True,
            'periodo': mes.strftime('%Y-%m'),
            'archivos_procesados': archivos_procesados,
            'archivos_error': archivos_error,
            'total_archivos': len(archivos_xml),
            'datos': resultado
        }