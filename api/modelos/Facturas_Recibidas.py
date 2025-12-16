import pyodbc
import logging
import re
from decimal import Decimal
from typing import List, Dict, Any, Optional
from api.db_connections import CONTROLGAS_CONN_STR

logger = logging.getLogger(__name__)

class FacturasRecibidas:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str
    
    def obtener_facturas_base(
        self, 
        from_date: str, 
        until_date: str,
        codgas: str = '0',
        proveedor: str = '0',
        company: str = '0'
    ) -> List[Dict[str, Any]]:
        """
        Obtiene el listado base de facturas recibidas con sus relaciones.
        
        Args:
            from_date: Fecha inicial (YYYY-MM-DD)
            until_date: Fecha final (YYYY-MM-DD)
            codgas: Código de estación (0 = todas)
            proveedor: Nombre del proveedor (0 = todos)
            company: Empresa (0 = ambas, 1 = TotalGas, 2 = Petrotal)
        
        Returns:
            Lista de diccionarios con información de facturas
        """
        try:
            # Construir filtros dinámicos
            filtros = ["fr.Fecha BETWEEN ? AND ?"]
            parametros = [from_date, until_date]
            
            # Filtro de proveedor
            if proveedor != '0':
                filtros.append("fr.EmisorNombre LIKE ?")
                parametros.append(f"%{proveedor}%")
            
            # Filtro de estación
            if codgas != '0':
                filtros.append("(fmt.codgas = ? OR fr.Destino LIKE ?)")
                parametros.append(int(codgas))
                parametros.append(f"%{codgas}%")
            
            # Filtro de empresa
            if company == '1':
                filtros.append("fr.ReceptorRfc NOT LIKE 'PET%'")
            elif company == '2':
                filtros.append("fr.ReceptorRfc LIKE 'PET%'")
            
            where_clause = " AND ".join(filtros)
            
            query = f"""
                SELECT 
                    -- DATOS BÁSICOS DE LA FACTURA
                    fr.Id as FacturaId,
                    fr.Folio as NumeroFacturaProveedorOriginal,
                    fr.Serie,
                    CONVERT(VARCHAR(10), fr.Fecha, 23) as FechaRecepcion,
                    fr.UUID,
                    
                    -- EMISOR (PROVEEDOR ORIGINAL)
                    fr.EmisorNombre as ProveedorOriginal,
                    fr.EmisorRfc as RfcProveedorOriginal,
                    
                    -- RECEPTOR
                    fr.ReceptorNombre as Receptor,
                    fr.ReceptorRfc,
                    
                    -- DETERMINAR EMPRESA
                    CASE 
                        WHEN fr.ReceptorRfc LIKE 'PET%' THEN 'Petrotal'
                        ELSE 'TotalGas'
                    END as Empresa,
                    
                    -- MONTOS
                    fr.SubTotal,
                    fr.Total as MontoFactura,
                    fr.TotalImpuestosTrasladados as IVATotal,
                    
                    -- OTROS DATOS
                    fr.FormaPago,
                    fr.MetodoPago,
                    fr.Moneda,
                    fr.Destino,
                    fr.Remision,
                    fr.LugarExpedicion,
                    fr.TipoDeComprobante,
                    
                    -- CONCEPTOS AGREGADOS (SUMAR LITROS)
                    ISNULL((
                        SELECT SUM(c.Cantidad)
                        FROM tg.dbo.FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id
                    ), 0) as LitrosDocumentoSoporte,
                    
                    -- PRIMER CONCEPTO (para obtener producto y precio unitario)
                    (
                        SELECT TOP 1 c.Descripcion
                        FROM tg.dbo.FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id
                        ORDER BY c.Id
                    ) as Producto,
                    
                    (
                        SELECT TOP 1 c.ValorUnitario
                        FROM tg.dbo.FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id
                        ORDER BY c.Id
                    ) as PrecioUnitario,
                    
                    (
                        SELECT TOP 1 c.TasaOCuota
                        FROM tg.dbo.FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id
                        ORDER BY c.Id
                    ) as TasaIVA,
                    
                    -- SUMA DE IEPS DE CONCEPTOS
                    ISNULL((
                        SELECT SUM(c.ImporteImpuesto)
                        FROM  tg.dbo.FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id 
                        AND c.Impuesto = 'IEPS'
                    ), 0) as IEPSTotal,
                    
                    -- RELACIÓN CON MOVIMIENTOS (si existe)
                    fmt.nrotrn as NumeroTransaccion,
                    fmt.codgas as CodigoEstacion,
                    fmt.TipoOperacion,
                    fmt.Observaciones as ObservacionesAsignacion,
                    
                    -- FACTURA PETROTAL (si es operación tipo 2)
                    frPetro.Folio as NumeroFacturaPetrotal,
                    frPetro.Total as MontoFacturaPetrotal,
                    fmt.LitrosPetrotal,
                    fmt.PrecioPetrotal,
                    
                    -- ESTADO
                    CASE 
                        WHEN fmt.Id IS NOT NULL THEN 'ASIGNADA'
                        ELSE 'PENDIENTE'
                    END as EstadoAsignacion
                    
                FROM tg.dbo.FacturasRecibidas fr
                
                -- LEFT JOIN con asignaciones a movimientos
                LEFT JOIN tg.dbo.FacturasMovimientosTanques fmt 
                    ON fr.Id = fmt.FacturaProveedorId 
                    AND fmt.Activo = 1
                
                -- LEFT JOIN con factura Petrotal (si aplica)
                LEFT JOIN tg.dbo.FacturasRecibidas frPetro 
                    ON fmt.FacturaPetrotalId = frPetro.Id
                
                WHERE {where_clause}
                
                ORDER BY fr.Fecha DESC, fr.Folio
            """
            
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query, parametros)
                
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                resultados = []
                for row in rows:
                    data_dict = dict(zip(columns, row))
                    
                    # Procesar y enriquecer datos
                    factura_procesada = self._procesar_factura(data_dict)
                    resultados.append(factura_procesada)
                
                logger.info(f"Facturas obtenidas: {len(resultados)}")
                return resultados
                
        except pyodbc.Error as e:
            logger.error(f"Error de base de datos en obtener_facturas_base: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado en obtener_facturas_base: {e}")
            raise
    
    def _procesar_factura(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Procesa y enriquece los datos de una factura.
        
        Args:
            data_dict: Diccionario con datos crudos de la factura
        
        Returns:
            Diccionario con datos procesados y campos calculados
        """
        # Extraer valores básicos
        litros = float(data_dict.get('LitrosDocumentoSoporte') or 0)
        monto = float(data_dict.get('MontoFactura') or 0)
        precio_unitario = float(data_dict.get('PrecioUnitario') or 0)
        ieps_total = float(data_dict.get('IEPSTotal') or 0)
        
        # PRECIO POR LITRO
        precio_x_litro = (monto / litros) if litros > 0 else 0
        
        # SALDO FACTURA (temporal - luego se calculará con pagos)
        saldo_factura = monto
        
        # Normalizar producto
        producto_raw = data_dict.get('Producto') or ''
        producto = self._normalizar_producto(producto_raw)
        
        # Normalizar proveedor
        proveedor_original = data_dict.get('ProveedorOriginal') or ''
        proveedor_normalizado = self._normalizar_proveedor(proveedor_original)
        
        # Extraer número de estación
        numero_estacion = self._extraer_numero_estacion(
            data_dict.get('CodigoEstacion'),
            data_dict.get('Destino')
        )
        
        # Nombre de estación (placeholder - se puede obtener de ma01)
        nombre_estacion = self._obtener_nombre_estacion(numero_estacion)
        
        # CAMPOS CALCULADOS Y ENRIQUECIDOS
        data_dict.update({
            'NumeroEstacion': numero_estacion,
            'NombreEstacion': nombre_estacion,
            'ProductoNormalizado': producto,
            'ProveedorNormalizado': proveedor_normalizado,
            'PrecioPorLitro': round(precio_x_litro, 6),
            'SaldoFactura': saldo_factura,
            'IEPSTotal': ieps_total,
            
            # PLACEHOLDERS - Se calcularán en fases posteriores
            'PrecioCotizado': 'PENDIENTE',
            'Diferencia': 0,
            'PrecioFacturaCotizadoPetrotal': 0,
            'PrecioVentaEstacion': 'PENDIENTE',
            'EstimadoFondoProveedor': 'PENDIENTE',
            'PorcentajeIVA': float(data_dict.get('TasaIVA') or 0) * 100,
            'Facturado': 'SI' if monto > 0 else 'NO',
            'UtilidadPorLitro': 0,
            'UtilidadLitros': 0,
            'PrecioClienteFinal': 'PENDIENTE',
            'DiferenciaVsTax': 0,
            'NumeroSuministros': 'PENDIENTE',
        })
        
        return data_dict
    
    def _normalizar_producto(self, producto_raw: str) -> str:
        """Normaliza el nombre del producto"""
        if not producto_raw:
            return 'N/A'
        
        prod = producto_raw.upper()
        
        if re.search(r'\b(REGULAR|MAGNA|87)\b', prod):
            return 'Regular'
        elif re.search(r'\b(PREMIUM|SUPER|91|93)\b', prod):
            return 'Premium'
        elif re.search(r'\b(DIESEL)\b', prod):
            return 'Diesel'
        else:
            return producto_raw[:50]
    
    def _normalizar_proveedor(self, proveedor_raw: str) -> str:
        """Normaliza el nombre del proveedor"""
        if not proveedor_raw:
            return 'N/A'
        
        prov = proveedor_raw.upper()
        
        if 'TESORO' in prov:
            return 'TESORO'
        elif 'MGC' in prov or 'MEXICO' in prov:
            return 'MGC'
        elif 'LOBO' in prov:
            return 'LOBO'
        elif 'PETROTAL' in prov:
            return 'PETROTAL'
        elif 'ESSAFUEL' in prov or 'ESSA' in prov:
            return 'ESSAFUEL'
        elif 'PREMIER' in prov:
            return 'PREMIERGAS'
        elif 'ENEREY' in prov:
            return 'ENEREY'
        elif 'AEMSA' in prov or 'ALTOS' in prov:
            return 'AEMSA'
        else:
            return proveedor_raw[:30]
    
    def _extraer_numero_estacion(
        self, 
        codigo_estacion: Optional[int], 
        destino: Optional[str]
    ) -> str:
        """Extrae el número de estación del código o del destino"""
        if codigo_estacion:
            return str(codigo_estacion)
        
        if destino:
            # Buscar patrón E##### en el destino
            m = re.search(r'E(\d{5})', destino)
            if m:
                return m.group(1)
            
            # Buscar solo números si son 4-5 dígitos
            m = re.search(r'\b(\d{4,5})\b', destino)
            if m:
                return m.group(1)
        
        return 'PENDIENTE'
    
    def _obtener_nombre_estacion(self, codigo_estacion: str) -> Optional[str]:
        """Obtiene el nombre de la estación desde la base de datos"""
        if codigo_estacion == 'PENDIENTE':
            return None
        
        try:
            query = """
                SELECT TOP 1 nomgas 
                FROM OPENQUERY([TG_WEB], 'SELECT codgas, nomgas FROM ma01')
                WHERE codgas = ?
            """
            
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query, [int(codigo_estacion)])
                row = cursor.fetchone()
                
                if row:
                    return row[0]
        except Exception as e:
            logger.warning(f"No se pudo obtener nombre de estación {codigo_estacion}: {e}")
        
        return None
    
    def obtener_factura_detalle(self, factura_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtiene el detalle completo de una factura específica.
        
        Args:
            factura_id: ID de la factura
        
        Returns:
            Diccionario con detalle completo o None si no existe
        """
        try:
            query = """
                SELECT 
                    fr.*,
                    -- Conceptos de la factura
                    (
                        SELECT 
                            c.Id,
                            c.Cantidad,
                            c.ClaveProdServ,
                            c.Descripcion,
                            c.ValorUnitario,
                            c.Importe,
                            c.Impuesto,
                            c.TasaOCuota,
                            c.ImporteImpuesto
                        FROM FacturasRecibidasConceptos c
                        WHERE c.FacturaId = fr.Id
                        FOR JSON PATH
                    ) as ConceptosJSON,
                    
                    -- Asignación a movimiento
                    fmt.nrotrn,
                    fmt.codgas,
                    fmt.TipoOperacion,
                    fmt.Observaciones
                    
                FROM FacturasRecibidas fr
                LEFT JOIN FacturasMovimientosTanques fmt 
                    ON fr.Id = fmt.FacturaProveedorId 
                    AND fmt.Activo = 1
                WHERE fr.Id = ?
            """
            
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query, [factura_id])
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                columns = [col[0] for col in cursor.description]
                data_dict = dict(zip(columns, row))
                
                # Parsear JSON de conceptos si existe
                import json
                if data_dict.get('ConceptosJSON'):
                    data_dict['Conceptos'] = json.loads(data_dict['ConceptosJSON'])
                    del data_dict['ConceptosJSON']
                else:
                    data_dict['Conceptos'] = []
                
                return data_dict
                
        except Exception as e:
            logger.error(f"Error al obtener detalle de factura {factura_id}: {e}")
            raise
    
    def obtener_estadisticas(
        self, 
        from_date: str, 
        until_date: str,
        **filtros
    ) -> Dict[str, Any]:
        """
        Obtiene estadísticas agregadas de facturas.
        
        Args:
            from_date: Fecha inicial
            until_date: Fecha final
            **filtros: Filtros adicionales (codgas, proveedor, company)
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            # Obtener facturas base
            facturas = self.obtener_facturas_base(
                from_date, 
                until_date,
                filtros.get('codgas', '0'),
                filtros.get('proveedor', '0'),
                filtros.get('company', '0')
            )
            
            # Calcular estadísticas
            total_facturas = len(facturas)
            total_litros = sum(f.get('LitrosDocumentoSoporte', 0) for f in facturas)
            total_monto = sum(f.get('MontoFactura', 0) for f in facturas)
            
            # Por proveedor
            por_proveedor = {}
            for f in facturas:
                prov = f.get('ProveedorNormalizado', 'N/A')
                if prov not in por_proveedor:
                    por_proveedor[prov] = {
                        'cantidad': 0,
                        'litros': 0,
                        'monto': 0
                    }
                por_proveedor[prov]['cantidad'] += 1
                por_proveedor[prov]['litros'] += f.get('LitrosDocumentoSoporte', 0)
                por_proveedor[prov]['monto'] += f.get('MontoFactura', 0)
            
            # Por producto
            por_producto = {}
            for f in facturas:
                prod = f.get('ProductoNormalizado', 'N/A')
                if prod not in por_producto:
                    por_producto[prod] = {
                        'cantidad': 0,
                        'litros': 0,
                        'monto': 0
                    }
                por_producto[prod]['cantidad'] += 1
                por_producto[prod]['litros'] += f.get('LitrosDocumentoSoporte', 0)
                por_producto[prod]['monto'] += f.get('MontoFactura', 0)
            
            # Por estado
            por_estado = {
                'ASIGNADA': 0,
                'PENDIENTE': 0
            }
            for f in facturas:
                estado = f.get('EstadoAsignacion', 'PENDIENTE')
                por_estado[estado] = por_estado.get(estado, 0) + 1
            
            return {
                'totales': {
                    'facturas': total_facturas,
                    'litros': round(total_litros, 2),
                    'monto': round(total_monto, 2)
                },
                'por_proveedor': por_proveedor,
                'por_producto': por_producto,
                'por_estado': por_estado
            }
            
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {e}")
            raise