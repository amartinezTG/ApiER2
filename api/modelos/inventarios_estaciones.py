import pyodbc
import logging
from decimal import Decimal
from api.db_connections import CONTROLGAS_CONN_STR

logger = logging.getLogger(__name__)

class InventariosEstaciones:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str
    
    def get_inventarios_estacion(self, servidor, base_datos, codigo_estacion, from_date, until_date):
        """
        Consulta inventarios de una estación específica usando OPENQUERY
        """
        # Escapar comillas simples en el query para OPENQUERY
        query = f"""
        DECLARE @from INT = {from_date};
        DECLARE @until INT = {until_date};
        DECLARE @codgas INT = {codigo_estacion};
        
        WITH VentasTotales AS (
            SELECT
                t2.codgas,
                t1.codprd,
                fch,
                ROUND(SUM(t1.canven), 3) AS Total
            FROM
                [{base_datos}].[dbo].[Ventas] t1
                LEFT JOIN [{base_datos}].[dbo].[Islas] t2 ON t1.codisl = t2.cod 
            WHERE
                fch BETWEEN @from AND @until 
                AND t1.codprd IN (179, 180, 181, 192, 193)
                AND t2.codgas = @codgas
            GROUP BY t2.codgas, t1.codprd, fch
        ),
        MovimientosTotales AS (
            SELECT 
                SUM(can) Total, 
                fch, 
                codgas,
                codprd
            FROM 
                [{base_datos}].[dbo].[Movimientos]
            WHERE 
                fch BETWEEN @from AND @until 
                AND codprd IN (179, 180, 181, 192, 193) 
                AND can > 0
                AND codgas = @codgas
            GROUP BY fch, codgas, codprd
        )
        SELECT
            t3.den Producto,
            ROUND(COALESCE(t2.Total, 0), 3) SaldoInicial,
            ROUND(COALESCE(t4.Total, 0), 3) AS Compras,
            COALESCE(t5.Total, 0) Ventas,
            ROUND(COALESCE((COALESCE(t2.Total, 0) + COALESCE(t4.Total, 0) - COALESCE(t5.Total, 0)), 0), 3) AS SaldoFinal,
            ROUND(COALESCE(t6.Total, 0), 3) SaldoReal,
            ROUND(COALESCE((COALESCE(t8.Total, 0) - (COALESCE(t9.Total, 0) + COALESCE(t4.Total, 0) - COALESCE(t5.Total, 0))), 0), 3) AS Merma,
            t2.codprd
        FROM
            [{base_datos}].[dbo].Gasolineras t1
            
            LEFT JOIN (
                SELECT 
                    fch,
                    codgas, 
                    codprd, 
                    Total
                FROM (
                    SELECT 
                        fch,
                        codgas, 
                        codprd, 
                        SUM(can) AS Total,
                        ROW_NUMBER() OVER (PARTITION BY codprd, codgas ORDER BY fch ASC) AS rn
                    FROM [{base_datos}].[dbo].[StockReal]
                    WHERE fch BETWEEN (@from - 1) AND @until
                        AND codprd IN (179, 180, 181, 192, 193) 
                        AND nrotur >= 40
                        AND codgas = @codgas
                    GROUP BY fch, codgas, codprd
                ) AS RankedResults
                WHERE rn = 1
            ) t2 ON t1.cod = t2.codgas
            
            LEFT JOIN [{base_datos}].[dbo].Productos t3 ON t2.codprd = t3.cod
            
            LEFT JOIN (
                SELECT codgas, codprd, SUM(can) Total 
                FROM [{base_datos}].[dbo].[Movimientos] 
                WHERE fch BETWEEN @from AND @until 
                AND codprd IN (179, 180, 181, 192, 193) 
                AND can > 0 
                AND codgas = @codgas
                GROUP BY codgas, codprd
            ) t4 ON t1.cod = t4.codgas AND t2.codprd = t4.codprd
            
            LEFT JOIN (
                SELECT t2.codgas, t1.codprd, ROUND(SUM(t1.canven), 3) AS Total 
                FROM [{base_datos}].[dbo].[Ventas] t1 
                LEFT JOIN [{base_datos}].[dbo].[Islas] t2 ON t1.codisl = t2.cod 
                WHERE t1.fch BETWEEN @from AND @until 
                AND t1.codprd IN (179, 180, 181, 192, 193)
                AND t2.codgas = @codgas
                GROUP BY codgas, codprd
            ) t5 ON t1.cod = t5.codgas AND t2.codprd = t5.codprd
            
            LEFT JOIN (
                SELECT 
                    codgas, 
                    codprd, 
                    Total
                FROM (
                    SELECT 
                        codgas, 
                        codprd, 
                        SUM(can) AS Total,
                        ROW_NUMBER() OVER (PARTITION BY codprd, codgas ORDER BY fch DESC) AS rn
                    FROM [{base_datos}].[dbo].[StockReal]
                    WHERE fch BETWEEN @from AND @until
                        AND codprd IN (179, 180, 181, 192, 193) 
                        AND nrotur >= 40
                        AND codgas = @codgas
                    GROUP BY codgas, codprd, fch
                ) AS RankedResults
                WHERE rn = 1
            ) t6 ON t1.cod = t6.codgas AND t2.codprd = t6.codprd
            
            LEFT JOIN (
                SELECT codgas, codprd, SUM(can) Total
                FROM [{base_datos}].[dbo].[StockReal] 
                WHERE fch BETWEEN @from AND @until
                AND codprd IN (179, 180, 181, 192, 193) 
                AND nrotur >= 40
                AND codgas = @codgas
                GROUP BY codgas, codprd
            ) t8 ON t1.cod = t8.codgas AND t2.codprd = t8.codprd
        
            LEFT JOIN (
                SELECT codgas, codprd, SUM(can) Total 
                FROM [{base_datos}].[dbo].[StockReal] 
                WHERE fch BETWEEN (@from - 1) AND (@until - 1)
                AND codprd IN (179, 180, 181, 192, 193) 
                AND nrotur >= 40
                AND codgas = @codgas
                GROUP BY codgas, codprd
            ) t9 ON t1.cod = t9.codgas AND t2.codprd = t9.codprd
        
        WHERE
            t1.cod = @codgas
        ORDER BY 
            Producto;
        """
        
        # Reemplazar comillas simples por comillas dobles para OPENQUERY
        query_escaped = query.replace("'", "''")
        
        sql = f"SELECT * FROM OPENQUERY([{servidor}], '{query_escaped}')"
        
        try:
            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                # Convertir a lista de diccionarios
                results = []
                for row in rows:
                    row_dict = {}
                    for idx, col in enumerate(cols):
                        value = row[idx]
                        # Convertir Decimal a float para JSON serialization
                        if isinstance(value, Decimal):
                            value = float(value)
                        row_dict[col] = value
                    results.append(row_dict)
                
                return results
                
        except pyodbc.Error as e:
            logger.error(f"Error SQL en estación {codigo_estacion} ({servidor}/{base_datos}): {str(e)}")
            print(f"Error SQL ejecutando consulta en estación {codigo_estacion}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error general en estación {codigo_estacion}: {str(e)}")
            print(f"Error ejecutando consulta: {e}")
            return []
    def get_detalles_estacion(self, servidor, base_datos, codigo_estacion, codigo_producto, from_date, until_date):
        """
        Consulta detalles diarios de inventarios de una estación específica
        """
        query = f"""
        DECLARE @from INT = {from_date};
        DECLARE @until INT = {until_date};
        DECLARE @codgas INT = {codigo_estacion}; 
        DECLARE @codprd INT = {codigo_producto};

        WITH VentasTotales AS (
            SELECT
                fch,
                ROUND(SUM(t1.canven), 3) AS Total
            FROM
                [{base_datos}].[dbo].[Ventas] t1
                LEFT JOIN [{base_datos}].[dbo].[Islas] t2 ON t1.codisl = t2.cod 
            WHERE
                t2.codgas = @codgas AND
                fch BETWEEN @from AND @until AND
                codprd = @codprd
            GROUP BY fch
        )
        SELECT 
            CONVERT(VARCHAR(10), DATEADD(day, -1, t1.fch), 23) as Fecha, 
            ROUND(COALESCE(
                LAG(t1.can) OVER (
                    PARTITION BY t1.codgas, t1.codprd 
                    ORDER BY t1.fch, t1.nrotur
                ), 
                0
            ), 3) AS SdoInicial,
            ISNULL(t3.Total, 0) AS Compras,
            t2.Total AS Ventas,
            ROUND((COALESCE(
                LAG(t1.can) OVER (
                    PARTITION BY t1.codgas, t1.codprd 
                    ORDER BY t1.fch, t1.nrotur
                ), 
                0
            ) + ISNULL(t3.Total, 0) - t2.Total), 3) AS Saldo_Final,
            ROUND(t1.can, 3) SaldoReal,
            ROUND((t1.can - (COALESCE(
                LAG(t1.can) OVER (
                    PARTITION BY t1.codgas, t1.codprd 
                    ORDER BY t1.fch, t1.nrotur
                ), 
                0
            ) + ISNULL(t3.Total, 0) - t2.Total)), 3) Merma,
            t4.abr Estacion,
            t5.den Producto
        FROM 
            [{base_datos}].[dbo].[StockReal] t1
            LEFT JOIN VentasTotales t2 ON t1.fch = t2.fch
            LEFT JOIN (
                SELECT TOP (1000) SUM(can) Total, fch
                FROM [{base_datos}].[dbo].[Movimientos]
                WHERE fch BETWEEN @from AND @until 
                    AND codgas = @codgas 
                    AND codprd = @codprd 
                    AND can > 0
                GROUP BY fch
            ) t3 ON t1.fch = t3.fch
            LEFT JOIN [{base_datos}].[dbo].[Gasolineras] t4 ON t1.codgas = t4.cod
            LEFT JOIN [{base_datos}].[dbo].[Productos] t5 ON t1.codprd = t5.cod
        WHERE
            t1.fch BETWEEN (@from-1) AND @until AND
            t1.codgas = @codgas AND
            t1.codprd = @codprd AND
            t1.nrotur >= 40
        """
        
        # Escapar comillas para OPENQUERY
        query_escaped = query.replace("'", "''")
        sql = f"SELECT * FROM OPENQUERY([{servidor}], '{query_escaped}')"
        
        try:
            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                # Convertir a lista de diccionarios
                results = []
                for row in rows:
                    row_dict = {}
                    for idx, col in enumerate(cols):
                        value = row[idx]
                        # Convertir Decimal a float
                        if isinstance(value, Decimal):
                            value = float(value)
                        row_dict[col] = value
                    results.append(row_dict)
                
                return results
                
        except pyodbc.Error as e:
            logger.error(f"Error SQL detalles estación {codigo_estacion}, producto {codigo_producto}: {str(e)}")
            print(f"Error SQL: {e}")
            return []
        except Exception as e:
            logger.error(f"Error general detalles: {str(e)}")
            print(f"Error ejecutando consulta detalles: {e}")
            return []
        

    def get_tanques_estacion(self, servidor, base_datos, codigo_estacion):
        """
        Obtiene la lista de tanques de una estación
        """
        query = f"""
        
            SELECT 
            cod,
            den as producto,
            nrotf1 as [numero_tan]
            from [{base_datos}].[dbo].Tanques
        """
        
        query_escaped = query.replace("'", "''")
        sql = f"SELECT * FROM OPENQUERY([{servidor}], '{query_escaped}')"
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error obteniendo tanques de estación {codigo_estacion}: {str(e)}")
            print(f"Error: {e}")
            return []

    def get_volumen_tanque(self, servidor, base_datos, codigo_estacion, codigo_tanque, limit=100):
        """
        Obtiene el historial de volumen de un tanque específico
        """
        query = f"""
        SELECT TOP({limit})
            CONVERT(VARCHAR(10), DATEADD(day, -1, t1.fchtrn), 23) as fecha, 
            CAST(CONVERT(TIME, DATEADD(MINUTE, t1.hratrn % 100, DATEADD(HOUR, t1.hratrn / 100, 0))) AS TIME(0)) AS hora,
            t2.capope + capfon as capacidad_maxima,
            t2.capope as capacidad_operativa,
            t2.caputi as util,
            t2.capfon as fondage,
            t2.volmin as volumen_min,
            t2.den as producto,
            t1.vol,
            t1.volCxT, 
            t1.volh2o
        FROM [{base_datos}].[dbo].MovimientosTan t1
        LEFT JOIN [{base_datos}].[dbo].Tanques t2 ON t1.codtan = t2.cod 
        WHERE t1.codgas = {codigo_estacion}
            AND t1.codtan = {codigo_tanque}
            AND tiptrn NOT IN(2,3,4)
             AND t1.vol > 0
             and t2.est =0
        ORDER BY t1.nrotrn DESC
        """
        
        query_escaped = query.replace("'", "''")
        sql = f"SELECT * FROM OPENQUERY([{servidor}], '{query_escaped}')"
        
        try:
            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    row_dict = {}
                    for idx, col in enumerate(cols):
                        value = row[idx]
                        if isinstance(value, Decimal):
                            value = float(value)
                        # Convertir datetime.time a string
                        elif hasattr(value, 'strftime'):
                            value = value.strftime('%H:%M:%S')
                        row_dict[col] = value
                    results.append(row_dict)
                
                return results
                
        except Exception as e:
            logger.error(f"Error obteniendo volumen tanque {codigo_tanque}: {str(e)}")
            print(f"Error: {e}")
            return []
        
    def get_consolidado_tanques(self, servidor, base_datos, codigo_estacion, from_date, until_date):
        """
        Obtiene reporte consolidado de máximos y mínimos de todos los tanques de una estación
        """
        query = f"""
        SELECT 
            t2.cod as codtan,
            t2.den as producto,
            t2.nrotf1 as numero_tan,
            t2.capope + t2.capfon as capacidad_maxima,
            t2.volmin as capacidad_minima,
            MAX(t1.vol) as vol_maximo,
            MIN(t1.vol) as vol_minimo,
            AVG(t1.vol) as vol_promedio,
            COUNT(*) as num_registros
        FROM [{base_datos}].[dbo].MovimientosTan t1
        LEFT JOIN [{base_datos}].[dbo].Tanques t2 ON t1.codtan = t2.cod
        WHERE t1.codgas = {codigo_estacion}
            AND CONVERT(VARCHAR(10), DATEADD(day, -1, t1.fchtrn), 23) BETWEEN '{from_date}' AND '{until_date}'
            AND t1.tiptrn NOT IN(2,3,4)
            AND t1.vol > 0
            and t1.vol < 1000000
            and t2.est =0
        GROUP BY t2.cod, t2.den, t2.nrotf1, t2.capope, t2.capfon, t2.volmin
        HAVING COUNT(*) > 0
        ORDER BY t2.nrotf1
        """
        
        query_escaped = query.replace("'", "''")
        sql = f"SELECT * FROM OPENQUERY([{servidor}], '{query_escaped}')"
        
        try:
            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    row_dict = {}
                    for idx, col in enumerate(cols):
                        value = row[idx]
                        if isinstance(value, Decimal):
                            value = float(value)
                        row_dict[col] = value
                    results.append(row_dict)
                
                return results
                
        except Exception as e:
            logger.error(f"Error consolidado tanques estación {codigo_estacion}: {str(e)}")
            print(f"Error: {e}")
            return []