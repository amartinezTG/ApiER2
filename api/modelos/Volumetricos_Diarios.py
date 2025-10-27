# api/modelos/Volumetricos_Diarios.py
import os
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import List, Dict, Optional
from collections import defaultdict
import platform

class VolumetricosDiarios:
    """Procesa archivos XML de controles volumétricos diarios desde directorio centralizado"""
    
    # Configuración del servidor central
    SERVIDOR_CENTRAL = "192.168.0.6"
    SHARE_CENTRAL = "Aplicativos"
    RUTA_BASE_VOLUMETRICOS = "RespaldoVolumetricosEstaciones"
    
    def __init__(self, usuario: str = "Administrador", password: str = "T0t4lG4s2020"):
        self.NS = {
            "cv": "http://www.sat.gob.mx/esquemas/controlesvolumetricos"
        }
        self.usuario = usuario
        self.password = password
        self._conexion_activa = False  # Solo una conexión al servidor central
    
    def _is_windows(self) -> bool:
        """Detecta si el sistema operativo es Windows"""
        return platform.system() == "Windows"

    def _mount_point(self) -> str:
        """Retorna el punto de montaje para Linux del servidor central"""
        return f"/mnt/{self.SHARE_CENTRAL}/{self.SERVIDOR_CENTRAL}"
    
    def conectar_recurso_red(self) -> bool:
        """
        Conecta al servidor central de volumétricos
        - En Windows: intenta montar con net use
        - En Linux: verifica que ya esté montado por el sistema
        
        Returns:
            True si la conexión está disponible
        """
        # Si ya está conectado, reutilizar
        if self._conexion_activa:
            return True
        
        if self._is_windows():
            # Lógica para Windows (net use)
            import subprocess
            recurso = f"\\\\{self.SERVIDOR_CENTRAL}\\{self.SHARE_CENTRAL}"
            
            try:
                comando = [
                    "net", "use", recurso,
                    f"/user:{self.usuario}", self.password,
                    "/persistent:no"
                ]
                
                resultado = subprocess.run(
                    comando,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if resultado.returncode == 0:
                    print(f"[INFO] Conectado exitosamente a {recurso}")
                    self._conexion_activa = True
                    return True
                else:
                    # Si ya está conectado, también es éxito
                    if "Multiple connections" in resultado.stderr or "already" in resultado.stderr.lower():
                        print(f"[INFO] Ya existe conexión a {recurso}")
                        self._conexion_activa = True
                        return True
                    
                    print(f"[ERROR] No se pudo conectar a {recurso}")
                    print(f"[ERROR] stderr: {resultado.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                print(f"[ERROR] Timeout al conectar a {recurso}")
                return False
            except Exception as e:
                print(f"[ERROR] Excepción al conectar a {recurso}: {e}")
                return False
        else:
            # En Linux: verificar que ya esté montado por el sistema
            mnt = self._mount_point()
            if os.path.ismount(mnt):
                print(f"[INFO] Share ya montado en {mnt}")
                self._conexion_activa = True
                return True
            
            print(f"[ERROR] Share no montado en {mnt}. Monta con CIFS (ver instrucciones).")
            return False
    
    def desconectar_recurso_red(self) -> bool:
        """
        Desconecta el recurso de red (solo en Windows)
        En Linux no desmontamos desde la app
        
        Returns:
            True si se desconectó exitosamente
        """
        if not self._conexion_activa:
            return True
        
        if self._is_windows():
            import subprocess
            recurso = f"\\\\{self.SERVIDOR_CENTRAL}\\{self.SHARE_CENTRAL}"
            
            try:
                comando = ["net", "use", recurso, "/delete", "/yes"]
                resultado = subprocess.run(
                    comando,
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                if resultado.returncode == 0 or "not found" in resultado.stderr.lower():
                    print(f"[INFO] Desconectado de {recurso}")
                    self._conexion_activa = False
                    return True
                else:
                    print(f"[WARNING] No se pudo desconectar de {recurso}: {resultado.stderr}")
                    return False
                    
            except Exception as e:
                print(f"[ERROR] Error al desconectar de {recurso}: {e}")
                return False
        
        # En Linux no desmontamos desde la aplicación
        self._conexion_activa = False
        return True
    
    def construir_path_volumetricos(self, permiso_cre: str, mes: date) -> str:
        """
        Construye la ruta para acceder a los XML diarios de una estación
        Formato: \\192.168.0.6\Aplicativos\RespaldoVolumetricosEstaciones\YYYY\MM\PermisoCRE
        
        Args:
            permiso_cre: Permiso CRE de la estación (ej: "PL_19422_EXP_ES_2016")
            mes: Fecha del mes a consultar
        Returns:
            Ruta completa al directorio de volumétricos
        """
        year = mes.strftime("%Y")
        month = mes.strftime("%m")
        
        # Limpiar el permiso CRE (remover caracteres problemáticos)
        permiso_limpio = permiso_cre.replace('/', '_').strip()
        
        if self._is_windows():
            base = f"\\\\{self.SERVIDOR_CENTRAL}\\{self.SHARE_CENTRAL}"
            return f"{base}\\{self.RUTA_BASE_VOLUMETRICOS}\\{year}\\{month}\\{permiso_limpio}"
        else:
            base = self._mount_point()
            return os.path.join(base, self.RUTA_BASE_VOLUMETRICOS, year, month, permiso_limpio)
    
    def obtener_archivos_xml_mes(self, permiso_cre: str, mes: date) -> List[str]:
        """
        Obtiene lista de archivos XML del mes especificado para una estación
        
        Args:
            permiso_cre: Permiso CRE de la estación
            mes: Fecha del mes a consultar
        Returns:
            Lista de rutas completas a archivos XML
        """
        if not self.conectar_recurso_red():
            print(f"[ERROR] No se pudo establecer conexión con el servidor central {self.SERVIDOR_CENTRAL}")
            return []

        path = self.construir_path_volumetricos(permiso_cre, mes)
        
        try:
            if not os.path.exists(path):
                print(f"[WARNING] No existe el directorio: {path}")
                return []
            
            archivos = []
            for archivo in os.listdir(path):
                if archivo.lower().endswith('.xml'):
                    archivos.append(os.path.join(path, archivo))
            
            print(f"[INFO] Encontrados {len(archivos)} archivos XML en {path}")
            return sorted(archivos)
            
        except PermissionError:
            print(f"[ERROR] Sin permisos para acceder a {path}")
            print(f"[INFO] Usuario utilizado: {self.usuario}")
            return []
        except Exception as e:
            print(f"[ERROR] Error al listar archivos en {path}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _to_float(self, val) -> float:
        """Convierte un valor a float, retorna 0.0 si falla"""
        try:
            return float(val) if val else 0.0
        except (TypeError, ValueError):
            return 0.0
    
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
                    
                    volumen = self._to_float(cabecera.get('sumatoriaVolumenDespachado'))
                    ventas = self._to_float(cabecera.get('sumatoriaVentas'))
                    
                    ventas_por_producto[key]['volumen_total'] += volumen
                    ventas_por_producto[key]['importe_total'] += ventas
                
                # Procesar VTADetalle
                for detalle in vta.findall('.//cv:VTADetalle', self.NS):
                    clave_producto = detalle.get('claveProducto')
                    clave_subproducto = detalle.get('claveSubProducto')
                    key = f"{clave_producto}-{clave_subproducto}"
                    
                    volumen = self._to_float(detalle.get('volumenDespachado'))
                    importe = self._to_float(detalle.get('importeTotalTransaccion'))

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
    
    def generar_resumen_mensual(self, permiso_cre: str, mes: date, desconectar_al_final: bool = False) -> Dict:
        """
        Genera resumen mensual consolidado de todos los XMLs diarios de una estación
        
        Args:
            permiso_cre: Permiso CRE de la estación
            mes: Fecha del mes a procesar
            desconectar_al_final: Si debe desconectar el recurso al terminar
        Returns:
            Diccionario con resumen consolidado
        """
        archivos_xml = self.obtener_archivos_xml_mes(permiso_cre, mes)
        print(f"[INFO] Total archivos XML encontrados: {len(archivos_xml)}")

        if not archivos_xml:
            if desconectar_al_final:
                self.desconectar_recurso_red()
            return {
                'success': False,
                'mensaje': 'No se encontraron archivos XML',
                'permiso_cre': permiso_cre,
                'periodo': mes.strftime('%Y-%m'),
                'datos': [],
                'archivos_procesados': 0,
                'archivos_error': 0,
                'total_archivos': 0
            }

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
                    if datos_dia.get('fecha_corte'):
                        consolidado[producto]['dias_con_venta'].add(datos_dia['fecha_corte'][:10])
                    consolidado[producto]['detalle_diario'].append({
                        'archivo': datos_dia['archivo'],
                        'fecha': datos_dia.get('fecha_corte'),
                        'volumen': ventas['volumen_total'],
                        'importe': ventas['importe_total'],
                        'transacciones': ventas['num_transacciones']
                    })
            else:
                archivos_error += 1

        if desconectar_al_final:
            self.desconectar_recurso_red()

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
            'permiso_cre': permiso_cre,
            'archivos_procesados': archivos_procesados,
            'archivos_error': archivos_error,
            'total_archivos': len(archivos_xml),
            'datos': resultado
        }
    
    def __del__(self):
        """Cleanup: desconectar recurso al destruir el objeto"""
        try:
            self.desconectar_recurso_red()
        except:
            pass