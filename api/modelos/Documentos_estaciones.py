import pyodbc
from api.db_connections import CONTROLGAS_CONN_STR
from datetime import datetime, timedelta

class DocumentosEstaciones:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str


    def get_purchase_from_station(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
        # Arma la consulta interna (la de OPENQUERY)
        prov = int(proveedor or 0)
        proveedor_filter = f" AND t4.cod = {prov}" if prov != 0 else ""
        inner_query = f"""
            SELECT
                t1.nro,
                CASE 
                    WHEN CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) > 0 THEN
                        SUBSTRING(
                            CAST(t1.txtref AS VARCHAR(MAX)),
                            CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3,
                            CHARINDEX('@', CAST(t1.txtref AS VARCHAR(MAX)) + '@', CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
                            - (CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
                        )
                    ELSE NULL
                END AS Factura,
                CASE 
                    WHEN CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) > 0 THEN
                        SUBSTRING(
                            CAST(t1.txtref AS VARCHAR(MAX)),
                            CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3,
                            CHARINDEX('@',
                                CAST(t1.txtref AS VARCHAR(MAX)) + '@',
                                CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3
                            )
                            - (CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
                        )
                    ELSE NULL
                END AS Remision,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.fch), 23) AS fecha,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.vto), 23) AS fechaVto,
                t3.den as [producto],
                t4.den as [proveedor],
                t8.volrec,
                t2.can,
                t2.pre,
                (t2.mto/100) as [mto],
                (t2.mtoiie/100) as [mtoiie],
                (t2.mtoiva/100) as [iva8],
                (t5.mto/100) as [iva],
                ((isnull(t2.mtoiva,0) + isnull(t5.mto,0))/100) as [iva_total],
                t6.mto as [servicio],
                t7.mto as [iva_servicio],
                ((t2.mto + (isnull(t2.mtoiva,0) + isnull(t5.mto,0)) + isnull(t6.mto,0)+isnull(t7.mto,0))/100) as [total_fac],
                t1.satuid,
                t1.codgas,
                t9.abr as [gasolinera]
            FROM [{short_db}].[dbo].DocumentosC t1
        LEFT JOIN [{short_db}].[dbo].Documentos t2 ON t1.nro =t2.nro and t1.codgas = t2.codgas and t2.codcpt in(1,2,3)
        LEFT JOIN [{short_db}].[dbo].Documentos t5 ON t1.nro =t5.nro and t1.codgas = t5.codgas and t5.codcpt in(21,22,23)
        LEFT JOIN [{short_db}].[dbo].Documentos t6 ON t1.nro =t6.nro and t1.codgas = t6.codgas and t6.codcpt in(18,19,20)
        LEFT JOIN [{short_db}].[dbo].Documentos t7 ON t1.nro =t7.nro and t1.codgas = t7.codgas and t7.codcpt in(24,25,26)
        LEFT JOIN [{short_db}].[dbo].Productos t3 ON t2.codprd =t3.cod
        LEFT JOIN [{short_db}].[dbo].Proveedores t4 on t1.codopr =t4.cod
        LEFT JOIN [{short_db}].[dbo].Gasolineras t9 on t1.codgas =t9.cod
        LEFT JOIN (SELECT sum(volrec) as volrec, nrodoc  FROM [{short_db}].[dbo].[MovimientosTan] where  tiptrn = 4 group by nrodoc) t8 on t1.nro = t8.nrodoc 
            WHERE 
            t1.tip = 1 
            AND t1.subope = 2 
            AND t1.fch BETWEEN '{from_date}' AND '{until_date}'
            {proveedor_filter}
            order by t1.nro asc
        """

        # Quita los saltos de línea y reemplaza comillas simples dobles
        inner_query = inner_query.replace("'", "''")
        sql = f"SELECT * FROM OPENQUERY([{linked_server}], '{inner_query}')"
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            print(f"Error ejecutando documentos estaciones para {codgas}: {e}")
            return []

