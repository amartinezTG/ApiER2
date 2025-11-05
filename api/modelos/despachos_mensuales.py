# api/modelos/despachos_mensuales.py
import pyodbc
from datetime import date, timedelta
from typing import List, Dict, Optional
from api.db_connections import CONTROLGAS_CONN_STR



class DespachosMensuales:
    """Modelo para consultar despachos mensuales por estación"""
    
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str
    def obtener_resumen_productos(self, servidor,base_datos,fecha_inicial, fecha_final,nombre_estacion) -> List[Dict]:
        sql = f"""
        DECLARE @Inicial date = '{fecha_inicial.replace(day=1)}';
        DECLARE @Final   date = '{fecha_final}';
        DECLARE @fini int  = DATEDIFF(day,0,@Inicial)+1;
        DECLARE @ffin int  = DATEDIFF(day,0,DATEADD(day,1,@Final))+1;

        WITH entregas AS (
            SELECT
                d.codprd,
                SUM(d.mto) AS monto,                                  -- Importe total de entregas (todas, sin gasfac)
                SUM(d.can) AS SumaVolumenEntregadoMes_ValorNumerico,  -- Volumen total entregado (todas)
                COUNT(*)   AS TotalEntregasMes                        -- Nº de despachos (todas)
            FROM [{servidor}].[{base_datos}].dbo.Despachos d
            WHERE d.fchcor BETWEEN @fini AND @ffin
            AND d.codprd IN (179,180,181,192,193)
            AND d.mto != 0
            AND d.tiptrn NOT IN (74,65)
            GROUP BY d.codprd
        ),
        per_doc AS (   -- paso intermedio: agrupar por nrofac,codprd solo los que sí tienen CFDI
            SELECT
                d.codprd,
                d.nrofac,
                SUM(d.can)  AS cantidad,
                SUM(d.mto)  AS importe,
                COUNT(*)    AS despachos
            FROM [{servidor}].[{base_datos}].dbo.Despachos d
            WHERE d.fchcor BETWEEN @fini AND @ffin
            AND d.codprd IN (179,180,181,192,193)
            AND d.mto != 0
            AND d.tiptrn NOT IN (74,65)
            AND d.gasfac != 0
            GROUP BY d.nrofac, d.codprd
        ),
        docs AS (      -- ahora sumar por codprd (visión por documento)
            SELECT
                p.codprd,
                COUNT(*)            AS documentos,                 -- Nº de documentos (CFDIs) en el mes
                SUM(p.importe)      AS ImporteTotalEntregasMes,    -- Monto total de esos documentos
                SUM(p.cantidad)     AS SumaVolumenCFDIs,           -- Volumen total de esos documentos
                SUM(p.despachos)    AS TotalDocumentosMes          -- Total de renglones-despacho que integran esos docs
            FROM per_doc p
            GROUP BY p.codprd
        )
        SELECT
        '{nombre_estacion}' AS Estación,
            COALESCE(e.codprd, c.codprd) AS codprd,
            CASE
                WHEN COALESCE(e.codprd, c.codprd) IN (179,192) THEN 'T-Maxima Regular'
                WHEN COALESCE(e.codprd, c.codprd) IN (180,193) THEN 'T-Super Premium'
                WHEN COALESCE(e.codprd, c.codprd) = 181        THEN 'Diesel Automotriz'
            END AS Producto,
            -- Métricas de ENTREGAS (todas, sin exigir gasfac)
            e.monto,
            e.SumaVolumenEntregadoMes_ValorNumerico,
            e.TotalEntregasMes,
            -- Métricas por DOCUMENTO (solo con gasfac != 0)
            c.ImporteTotalEntregasMes,
            c.SumaVolumenCFDIs,
            c.documentos,
            c.TotalDocumentosMes,
            Origen = 'DB_despachos'
        FROM entregas e
        FULL OUTER JOIN docs c
            ON c.codprd = e.codprd
        ORDER BY codprd;

        """
        
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []