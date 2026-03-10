import pyodbc
from api.db_connections import CONTROLGAS_CONN_STR
from datetime import datetime, timedelta

class DocumentosEstaciones:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str


    def get_purchase_from_station(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
        # Arma la consulta interna (la de OPENQUERY) - SIN EL JOIN A TG
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
                END  COLLATE Modern_Spanish_CI_AS AS Factura,
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
                END COLLATE Modern_Spanish_CI_AS AS Remision,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.fch), 23) AS fecha,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.vto), 23) AS fechaVto,
                CASE
                    WHEN t3.den IN ('   T-Maxima Regular', ' Gasolina Regular Menor a 91 Octanos') THEN 'Regular'
                    WHEN t3.den IN ('   T-Super Premium', ' Gasolina Premium Mayor o Igual a 91 Octanos') THEN 'Super'
                    WHEN t3.den IN ('   Diesel Automotriz','Diesel Automotriz') THEN 'Diesel'
                END AS producto,
                t4.den as [proveedor],
                t4.cod as [proveedor_codigo],
                t8.volrec,
                t2.can,
                t2.pre,
                (t2.mto/100) as [mto],
                (t2.mtoiie/100) as [mtoiie],
                (t2.mtoiva/100) as [iva8],
                (t5.mto/100) as [iva],
                ((isnull(t2.mtoiva,0) + isnull(t5.mto,0))/100) as [iva_total],
                (t6.mto/100) as [servicio],
                (t7.mto/100) as [iva_servicio],
                ((t2.mto + (isnull(t2.mtoiva,0) + isnull(t5.mto,0)) + isnull(t6.mto,0)+isnull(t7.mto,0))/100) as [total_fac],
                t1.satuid,
                t1.codgas,
                t9.abr as [gasolinera],
                t9.codemp as [codigo_empresa]
            FROM [{short_db}].[dbo].DocumentosC t1
            LEFT JOIN [{short_db}].[dbo].Documentos t2 ON t1.nro =t2.nro and t1.codgas = t2.codgas and t2.codcpt in(1,2,3)
            LEFT JOIN [{short_db}].[dbo].Documentos t5 ON t1.nro =t5.nro and t1.codgas = t5.codgas and t5.codcpt in(21,22,23)
            LEFT JOIN [{short_db}].[dbo].Documentos t6 ON t1.nro =t6.nro and t1.codgas = t6.codgas and t6.codcpt in(18,19,20)
            LEFT JOIN [{short_db}].[dbo].Documentos t7 ON t1.nro =t7.nro and t1.codgas = t7.codgas and t7.codcpt in(24,25,26)
            LEFT JOIN [{short_db}].[dbo].Productos t3 ON t2.codprd =t3.cod
            LEFT JOIN [{short_db}].[dbo].Proveedores t4 on t1.codopr =t4.cod
            LEFT JOIN [{short_db}].[dbo].Gasolineras t9 on t1.codgas =t9.cod
            LEFT JOIN (SELECT sum(volrec) as volrec, nrodoc FROM [{short_db}].[dbo].[MovimientosTan] where tiptrn = 4 group by nrodoc) t8 on t1.nro = t8.nrodoc
            WHERE 
                t1.tip = 1 
                AND t1.subope = 2 
                and t4.cod !=55  -- Excluir proveedor Petrotal
                AND t1.fch BETWEEN '{from_date}' AND '{until_date}'
                {proveedor_filter}
            order by t1.nro asc
        """

        # Quita los saltos de línea y reemplaza comillas simples dobles
        inner_query = inner_query.replace("'", "''")

        # ← AQUÍ ESTÁ LA MAGIA: JOIN DESPUÉS DEL OPENQUERY
        sql = f"""
            SELECT 
                remote.*,
                local.id as payment_invoice_id,
                local.payment_request_id,
                local.status as payment_status,
                local.paid_amount,
                CASE 
                    WHEN local.uuid IS NOT NULL THEN 1
                    ELSE 0
                END as en_orden_pago,
                t3.dias_credito,
                DATEADD(
                    DAY,
                    ISNULL(t3.dias_credito, 0),
                    CONVERT(date, remote.fecha, 23)
                ) AS fecha_vencimiento_credito
            FROM OPENQUERY([{linked_server}], '{inner_query}') remote
            LEFT JOIN [TG].[dbo].[payment_request_invoices] local ON remote.satuid = local.uuid COLLATE Modern_Spanish_CI_AS
            LEFT JOIN [TG].dbo.Proveedores t3 on t3.id_control_gas = remote.proveedor_codigo
        """
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

    def analisis_de_compras(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
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
                END COLLATE Modern_Spanish_CI_AS AS Factura,
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
                END COLLATE Modern_Spanish_CI_AS AS Remision,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.fch), 23) AS fecha,
                CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.vto), 23) AS fechaVto,
                CASE
                    WHEN t3.den IN ('   T-Maxima Regular', ' Gasolina Regular Menor a 91 Octanos') THEN 'Regular'
                    WHEN t3.den IN ('   T-Super Premium', ' Gasolina Premium Mayor o Igual a 91 Octanos') THEN 'Super'
                    WHEN t3.den IN ('   Diesel Automotriz','Diesel Automotriz') THEN 'Diesel'
                END AS producto,
                t4.den AS [proveedor],
                t4.cod AS [proveedor_codigo],
                t8.volrec,
                d.can,
                d.pre,
                (d.mto / 100) AS [mto],
                (d.mtoiie / 100) AS [mtoiie],
                (d.iva8 / 100) AS [iva8],
                (d.iva / 100) AS [iva],
                ((ISNULL(d.iva8, 0) + ISNULL(d.iva, 0)) / 100) AS [iva_total],
                (d.servicio / 100) AS [servicio],
                (d.iva_servicio / 100) AS [iva_servicio],
                ((d.mto + ISNULL(d.iva8, 0) + ISNULL(d.iva, 0) + ISNULL(d.servicio, 0) + ISNULL(d.iva_servicio, 0)) / 100) AS [total_fac],
                t1.satuid,
                t1.codgas,
                t9.abr AS [gasolinera],
                t9.codemp AS [codigo_empresa],
                t4.rfc AS [rfc]
            FROM [{short_db}].[dbo].DocumentosC t1
            LEFT JOIN (
                SELECT 
                    nro,
                    codgas,
                    SUM(CASE WHEN codcpt IN (1,2,3)    THEN can    ELSE 0 END) AS can,
                    SUM(CASE WHEN codcpt IN (1,2,3)    THEN pre    ELSE 0 END) AS pre,
                    SUM(CASE WHEN codcpt IN (1,2,3)    THEN mto    ELSE 0 END) AS mto,
                    SUM(CASE WHEN codcpt IN (1,2,3)    THEN mtoiie ELSE 0 END) AS mtoiie,
                    SUM(CASE WHEN codcpt IN (1,2,3)    THEN mtoiva ELSE 0 END) AS iva8,
                    SUM(CASE WHEN codcpt IN (21,22,23) THEN mto    ELSE 0 END) AS iva,
                    SUM(CASE WHEN codcpt IN (18,19,20) THEN mto    ELSE 0 END) AS servicio,
                    SUM(CASE WHEN codcpt IN (24,25,26) THEN mto    ELSE 0 END) AS iva_servicio,
                    MAX(CASE WHEN codcpt IN (1,2,3)    THEN codprd END) AS codprd
                FROM [{short_db}].[dbo].Documentos
                WHERE tip = 1
                  AND codcpt IN (1,2,3,18,19,20,21,22,23,24,25,26)
                GROUP BY nro, codgas
            ) d ON t1.nro = d.nro AND t1.codgas = d.codgas
            LEFT JOIN [{short_db}].[dbo].Productos t3 ON d.codprd = t3.cod
            LEFT JOIN [{short_db}].[dbo].Proveedores t4 ON t1.codopr = t4.cod
            LEFT JOIN [{short_db}].[dbo].Gasolineras t9 ON t1.codgas = t9.cod
            LEFT JOIN (
                SELECT SUM(volrec) AS volrec, nrodoc 
                FROM [{short_db}].[dbo].[MovimientosTan] mt
                WHERE mt.tiptrn = 4
                  AND EXISTS (
                      SELECT 1 
                      FROM [{short_db}].[dbo].DocumentosC dc
                      WHERE dc.nro    = mt.nrodoc
                        AND dc.tip    = 1
                        AND dc.subope = 2
                        AND dc.fch BETWEEN '{from_date}' AND '{until_date}'
                  )
                GROUP BY nrodoc
            ) t8 ON t1.nro = t8.nrodoc
            WHERE
                t1.tip = 1
                AND t1.subope = 2
                AND t1.fch BETWEEN '{from_date}' AND '{until_date}'
                {proveedor_filter}
            ORDER BY t1.nro ASC
        """

        # Escapar comillas simples para OPENQUERY
        inner_query = inner_query.replace("'", "''")

        sql = f"""
            SELECT 
                remote.nro,
                t4.nro AS nro_corp,
                remote.Factura,
                CASE 
                    WHEN CHARINDEX('@F:', CAST(t4.txtref AS VARCHAR(MAX))) > 0 THEN
                        SUBSTRING(
                            CAST(t4.txtref AS VARCHAR(MAX)),
                            CHARINDEX('@F:', CAST(t4.txtref AS VARCHAR(MAX))) + 3,
                            CHARINDEX('@', CAST(t4.txtref AS VARCHAR(MAX)) + '@', CHARINDEX('@F:', CAST(t4.txtref AS VARCHAR(MAX))) + 3)
                            - (CHARINDEX('@F:', CAST(t4.txtref AS VARCHAR(MAX))) + 3)
                        )
                    ELSE NULL
                END COLLATE Modern_Spanish_CI_AS AS Factura_corpo,
                remote.proveedor,
                t5.den AS proveedor_corpo,
                t4.satuid as uuid_corp,
                remote.Remision,
                remote.fecha,
                remote.fechaVto,
                remote.producto,
                remote.proveedor_codigo,
                remote.volrec,
                remote.can,
                remote.pre,
                remote.mto,
                remote.mtoiie,
                remote.iva8,
                remote.iva,
                remote.iva_total,
                remote.servicio,
                remote.iva_servicio,
                remote.total_fac,
                remote.satuid,
                remote.codgas,
                remote.gasolinera,
                remote.codigo_empresa,
                remote.rfc,
                local.id AS payment_invoice_id,
                local.payment_request_id,
                local.status AS payment_status,
                local.paid_amount,
                CASE 
                    WHEN local.uuid IS NOT NULL THEN 1
                    ELSE 0
                END AS en_orden_pago,
                t3.dias_credito,
                DATEADD(
                    DAY,
                    ISNULL(t3.dias_credito, 0),
                    CONVERT(DATE, remote.fecha, 23)
                ) AS fecha_vencimiento_credito,
                fr.Id AS factura_recibida_id,
                fr.EmisorNombre,
                fr.RutaArchivo,
                fr.NombreArchivo,
                t4.codopr,
                t4.satuid AS satuid_corp,
                t4.nro AS nro_corp_2
            FROM OPENQUERY([{linked_server}], '{inner_query}') remote
            LEFT JOIN [TG].[dbo].[payment_request_invoices] local 
                ON remote.satuid = local.uuid COLLATE Modern_Spanish_CI_AS
            LEFT JOIN [TG].[dbo].Proveedores t3 
                ON t3.id_control_gas = remote.proveedor_codigo
            LEFT JOIN [TG].[dbo].FacturasRecibidas fr 
                ON remote.satuid = fr.UUID COLLATE Modern_Spanish_CI_AS
            LEFT JOIN sg12.dbo.DocumentosC t4 
                ON remote.codgas = t4.codgas 
                AND remote.nro = t4.nro 
                AND t4.tip = 1 
            LEFT JOIN sg12.[dbo].Proveedores t5 
                ON t4.codopr = t5.cod
        """
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



    # def analisis_de_compras(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
    #     # Arma la consulta interna (la de OPENQUERY) - SIN EL JOIN A TG
    #     prov = int(proveedor or 0)
    #     proveedor_filter = f" AND t4.cod = {prov}" if prov != 0 else ""
    #     inner_query = f"""
    #         SELECT
    #             t1.nro,
    #             CASE 
    #                 WHEN CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) > 0 THEN
    #                     SUBSTRING(
    #                         CAST(t1.txtref AS VARCHAR(MAX)),
    #                         CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3,
    #                         CHARINDEX('@', CAST(t1.txtref AS VARCHAR(MAX)) + '@', CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
    #                         - (CHARINDEX('@F:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
    #                     )
    #                 ELSE NULL
    #             END  COLLATE Modern_Spanish_CI_AS AS Factura,
    #             CASE 
    #                 WHEN CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) > 0 THEN
    #                     SUBSTRING(
    #                         CAST(t1.txtref AS VARCHAR(MAX)),
    #                         CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3,
    #                         CHARINDEX('@',
    #                             CAST(t1.txtref AS VARCHAR(MAX)) + '@',
    #                             CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3
    #                         )
    #                         - (CHARINDEX('@R:', CAST(t1.txtref AS VARCHAR(MAX))) + 3)
    #                     )
    #                 ELSE NULL
    #             END COLLATE Modern_Spanish_CI_AS AS Remision,
    #             CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.fch), 23) AS fecha,
    #             CONVERT(VARCHAR(10), DATEADD(DAY, -1, t1.vto), 23) AS fechaVto,
    #             CASE
    #                 WHEN t3.den IN ('   T-Maxima Regular', ' Gasolina Regular Menor a 91 Octanos') THEN 'Regular'
    #                 WHEN t3.den IN ('   T-Super Premium', ' Gasolina Premium Mayor o Igual a 91 Octanos') THEN 'Super'
    #                 WHEN t3.den IN ('   Diesel Automotriz','Diesel Automotriz') THEN 'Diesel'
    #             END AS producto,
    #             t4.den as [proveedor],
    #             t4.cod as [proveedor_codigo],
    #             t8.volrec,
    #             t2.can,
    #             t2.pre,
    #             (t2.mto/100) as [mto],
    #             (t2.mtoiie/100) as [mtoiie],
    #             (t2.mtoiva/100) as [iva8],
    #             (t5.mto/100) as [iva],
    #             ((isnull(t2.mtoiva,0) + isnull(t5.mto,0))/100) as [iva_total],
    #             (t6.mto/100) as [servicio],
    #             (t7.mto/100) as [iva_servicio],
    #             ((t2.mto + (isnull(t2.mtoiva,0) + isnull(t5.mto,0)) + isnull(t6.mto,0)+isnull(t7.mto,0))/100) as [total_fac],
    #             t1.satuid,
    #             t1.codgas,
    #             t9.abr as [gasolinera],
    #             t9.codemp as [codigo_empresa],
    #             t4.rfc as [rfc] 
    #         FROM [{short_db}].[dbo].DocumentosC t1
    #         LEFT JOIN [{short_db}].[dbo].Documentos t2 ON t1.nro =t2.nro and t1.codgas = t2.codgas and t2.codcpt in(1,2,3) and t2.tip = 1
    #         LEFT JOIN [{short_db}].[dbo].Documentos t5 ON t1.nro =t5.nro and t1.codgas = t5.codgas and t5.codcpt in(21,22,23) and t2.tip = 1
    #         LEFT JOIN [{short_db}].[dbo].Documentos t6 ON t1.nro =t6.nro and t1.codgas = t6.codgas and t6.codcpt in(18,19,20) and t2.tip = 1
    #         LEFT JOIN [{short_db}].[dbo].Documentos t7 ON t1.nro =t7.nro and t1.codgas = t7.codgas and t7.codcpt in(24,25,26) and t2.tip = 1
    #         LEFT JOIN [{short_db}].[dbo].Productos t3 ON t2.codprd =t3.cod
    #         LEFT JOIN [{short_db}].[dbo].Proveedores t4 on t1.codopr =t4.cod
    #         LEFT JOIN [{short_db}].[dbo].Gasolineras t9 on t1.codgas =t9.cod
    #         LEFT JOIN (SELECT sum(volrec) as volrec, nrodoc FROM [{short_db}].[dbo].[MovimientosTan] where tiptrn = 4 group by nrodoc) t8 on t1.nro = t8.nrodoc
    #         WHERE
    #             t1.tip = 1
    #             AND t1.subope = 2
    #             AND t1.fch BETWEEN '{from_date}' AND '{until_date}'
    #             {proveedor_filter}
    #         order by t1.nro asc
    #     """

    #     # Quita los saltos de línea y reemplaza comillas simples dobles
    #     inner_query = inner_query.replace("'", "''")

    #     # ← AQUÍ ESTÁ LA MAGIA: JOIN DESPUÉS DEL OPENQUERY
    #     sql = f"""
    #         SELECT 
    #             remote.*,
    #             local.id as payment_invoice_id,
    #             local.payment_request_id,
    #             local.status as payment_status,
    #             local.paid_amount,
    #             CASE 
    #                 WHEN local.uuid IS NOT NULL THEN 1
    #                 ELSE 0
    #             END as en_orden_pago,
    #             t3.dias_credito,
    #             DATEADD(
    #                 DAY,
    #                 ISNULL(t3.dias_credito, 0),
    #                 CONVERT(date, remote.fecha, 23)
    #             ) AS fecha_vencimiento_credito,
    #             fr.Id as factura_recibida_id,fr.EmisorNombre,fr.RutaArchivo,fr.NombreArchivo
    #         FROM OPENQUERY([{linked_server}], '{inner_query}') remote
    #         LEFT JOIN [TG].[dbo].[payment_request_invoices] local ON remote.satuid = local.uuid COLLATE Modern_Spanish_CI_AS
    #         LEFT JOIN [TG].dbo.Proveedores t3 on t3.id_control_gas = remote.proveedor_codigo
    #         LEFT JOIN [TG].dbo.FacturasRecibidas fr on remote.satuid = fr.UUID COLLATE Modern_Spanish_CI_AS

            
    #     """
    #     try:
    #         with pyodbc.connect(self.conn_str) as conn:
    #             cursor = conn.cursor()
    #             cursor.execute(sql)
    #             cols = [col[0] for col in cursor.description]
    #             rows = cursor.fetchall()
    #         return [dict(zip(cols, row)) for row in rows]
    #     except Exception as e:
    #         print(f"Error ejecutando documentos estaciones para {codgas}: {e}")
    #         return []



    def get_resumen_movimientos_tanques(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
        """
        Obtiene el resumen de movimientos de tanques con información de facturas (directo e indirecto)
        """
        prov = int(proveedor or 0)
        proveedor_filter = f" AND t7.cod = {prov}" if prov != 0 else ""
        estacion_expr = "STUFF(t5.abr, 1, CHARINDEX(' ', t5.abr), '') AS [estacion]"

        inner_query = f"""
            SELECT
                CONVERT(VARCHAR(10), DATEADD(day, -1, t1.fchtrn), 23) as fecha,
                CAST(CONVERT(TIME, DATEADD(MINUTE, t1.hratrn % 100, DATEADD(HOUR, t1.hratrn / 100, 0))) AS TIME(0)) AS hora_formateada,
                t1.nrotrn,
                t1.codgas,
                t1.codgas as codgas_interno,
                t3.volrec as recaudado,
                t4.volrec as fac_rec,
                t4.nrodoc as nro_fac,
                t2.capmax,
                t2.den as combustible,
                {estacion_expr},
                t5.cveest as numero_estacion,
                t2.graprd,
                t6.satuid as uuid_original,
                t7.den as proveedor_controlgas,
                t8.monto as monto_factura_controlgas,
                t8.cantidad as cantidad_factura_controlgas,
                t8.precio as precio_factura_controlgas
            FROM [{short_db}].[dbo].MovimientosTan AS t1
            LEFT JOIN [{short_db}].[dbo].Tanques AS t2 ON t1.codtan = t2.cod
            LEFT JOIN [{short_db}].[dbo].MovimientosTan AS t3 on t1.nrotrn = t3.nrotrn and t3.tiptrn = 3
            LEFT JOIN [{short_db}].[dbo].MovimientosTan AS t4 on t1.nrotrn = t4.nrotrn and t4.tiptrn = 4 and t4.volrec != 0
            LEFT JOIN [{short_db}].[dbo].Gasolineras t5 on t1.codgas = t5.cod
            LEFT JOIN [{short_db}].[dbo].DocumentosC t6 on t4.nrodoc = t6.nro and tip = 1 and t6.codgas = t1.codgas
            LEFT JOIN [{short_db}].[dbo].Proveedores t7 on t6.codopr = t7.cod
            LEFT JOIN (
                SELECT 
                    nro, codgas,
                    SUM(mto) / 100 AS monto,
                    SUM(can) AS cantidad,
                    SUM(pre) as precio
                FROM [{short_db}].[dbo].Documentos
                WHERE tip = 1
                GROUP BY codgas, nro
            ) t8 ON t8.nro = t4.nrodoc AND t8.codgas = t1.codgas
            WHERE 
                t1.tiptrn in (2)
                AND t1.fchtrn BETWEEN '{from_date}' AND '{until_date}'
                {proveedor_filter}
            ORDER BY fecha ASC, hora_formateada ASC
        """

        inner_query = inner_query.replace("'", "''")
        
        # JOIN con las DOS posibles facturas
        sql = f"""
            SELECT 
                movimientos.*,

                -- INFORMACIÓN DE LA RELACIÓN
                fmt.Id as relacion_id,
                fmt.TipoOperacion as tipo_operacion,
                fmt.FechaAsignacion as fecha_asignacion,
                fmt.UsuarioAsignacion as usuario_asignacion,
                fmt.Observaciones as observaciones,

                -- FACTURA PROVEEDOR ORIGINAL
                fmt.FacturaProveedorId as factura_proveedor_id,
                fmt.UUIDProveedor as uuid_proveedor,
                fmt.FolioProveedor as folio_proveedor,
                fmt.MontoProveedor as monto_proveedor,
                fmt.LitrosProveedor as litros_proveedor,
                fmt.PrecioProveedor as precio_proveedor,
                frProv.Fecha as fecha_factura_proveedor,
                frProv.EmisorNombre as emisor_factura_proveedor,
                frProv.Total as total_factura_proveedor,
                frProv.Destino as destino_factura_proveedor,
                frProv.Remision as remision_factura_proveedor,
                
                -- FACTURA PETROTAL (puede ser NULL)
                fmt.FacturaPetrotalId as factura_petrotal_id,
                fmt.UUIDPetrotal as uuid_petrotal,
                fmt.FolioPetrotal as folio_petrotal,
                fmt.MontoPetrotal as monto_petrotal,
                fmt.LitrosPetrotal as litros_petrotal,
                fmt.PrecioPetrotal as precio_petrotal,
                frPetro.Fecha as fecha_factura_petrotal,
                frPetro.EmisorNombre as emisor_factura_petrotal,
                frPetro.Total as total_factura_petrotal,
                frPetro.Destino as destino_factura_petrotal,
                frPetro.Remision as remision_factura_petrotal,
                
                -- FLAGS
                CASE 
                    WHEN fmt.Id IS NOT NULL THEN 1 
                    ELSE 0 
                END as tiene_facturas_asignadas,
                
                CASE 
                    WHEN fmt.TipoOperacion = 2 THEN 1
                    ELSE 0
                END as es_operacion_petrotal,
                -- CÁLCULOS AUTOMÁTICOS (diferencias, márgenes)
                CASE 
                    WHEN fmt.TipoOperacion = 2 
                    THEN fmt.PrecioPetrotal - fmt.PrecioProveedor
                    ELSE NULL
                END as diferencia_precio,
                CASE 
                    WHEN fmt.TipoOperacion = 2 AND fmt.PrecioProveedor > 0
                    THEN ((fmt.PrecioPetrotal - fmt.PrecioProveedor) / fmt.PrecioProveedor) * 100
                    ELSE NULL
                END as margen_porcentual
            FROM OPENQUERY([{linked_server}], '{inner_query}') as movimientos
            LEFT JOIN [TG].[dbo].[FacturasMovimientosTanques] fmt ON movimientos.nrotrn = fmt.nrotrn AND movimientos.codgas = fmt.codgas AND fmt.Activo = 1
            LEFT JOIN [TG].[dbo].[FacturasRecibidas] frProv ON fmt.FacturaProveedorId = frProv.Id
            LEFT JOIN [TG].[dbo].[FacturasRecibidas] frPetro ON fmt.FacturaPetrotalId = frPetro.Id
            ORDER BY movimientos.fecha ASC, movimientos.hora_formateada ASC
        """
        
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

            return [dict(zip(cols, row)) for row in rows]

        except Exception as e:
            print(f"Error ejecutando consulta para estación {codgas}: {e}")
            return []


    def get_resumen_recepciones_combustible(self, linked_server, short_db, codgas, from_date, until_date, proveedor):
        """
        Obtiene el resumen de movimientos de tanques con información de facturas (directo e indirecto)
        """
        prov = int(proveedor or 0)
        proveedor_filter = f" AND t7.cod = {prov}" if prov != 0 else ""
        estacion_expr = "STUFF(t5.abr, 1, CHARINDEX(' ', t5.abr), '') AS [estacion]"

        inner_query = f"""
            SELECT
                CONVERT(VARCHAR(10), DATEADD(day, -1, t1.fchtrn), 23) as fecha,
                CAST(CONVERT(TIME, DATEADD(MINUTE, t1.hratrn % 100, DATEADD(HOUR, t1.hratrn / 100, 0))) AS TIME(0)) AS hora_formateada,
                t1.nrotrn,
                t1.codgas,
                t1.codgas as codgas_interno,
                t3.volrec as recaudado,
                t4.volrec as fac_rec,
                t4.nrodoc as nro_fac,
                t2.capmax,
                t2.den as combustible,
                {estacion_expr},
                t5.cveest as numero_estacion,
                t2.graprd,
                t6.satuid as uuid_original,
                CASE 
                    WHEN CHARINDEX('@F:', CAST(t6.txtref AS VARCHAR(MAX))) > 0 THEN
                        SUBSTRING(
                            CAST(t6.txtref AS VARCHAR(MAX)),
                            CHARINDEX('@F:', CAST(t6.txtref AS VARCHAR(MAX))) + 3,
                            CHARINDEX('@', CAST(t6.txtref AS VARCHAR(MAX)) + '@', CHARINDEX('@F:', CAST(t6.txtref AS VARCHAR(MAX))) + 3)
                            - (CHARINDEX('@F:', CAST(t6.txtref AS VARCHAR(MAX))) + 3)
                        )
                    ELSE NULL
                END AS factura_proveedor,
                CASE 
                    WHEN CHARINDEX('@R:', CAST(t6.txtref AS VARCHAR(MAX))) > 0 THEN
                        SUBSTRING(
                            CAST(t6.txtref AS VARCHAR(MAX)),
                            CHARINDEX('@R:', CAST(t6.txtref AS VARCHAR(MAX))) + 3,
                            CHARINDEX('@',
                                CAST(t6.txtref AS VARCHAR(MAX)) + '@',
                                CHARINDEX('@R:', CAST(t6.txtref AS VARCHAR(MAX))) + 3
                            )
                            - (CHARINDEX('@R:', CAST(t6.txtref AS VARCHAR(MAX))) + 3)
                        )
                    ELSE NULL
                END AS Remision,
                t7.den as proveedor_controlgas,
                t8.monto as monto_factura_controlgas,
                t8.cantidad as cantidad_factura_controlgas,
                t8.precio as precio_factura_controlgas
            FROM [{short_db}].[dbo].MovimientosTan AS t1
            LEFT JOIN [{short_db}].[dbo].Tanques AS t2 ON t1.codtan = t2.cod
            LEFT JOIN [{short_db}].[dbo].MovimientosTan AS t3 on t1.nrotrn = t3.nrotrn and t3.tiptrn = 3
            LEFT JOIN [{short_db}].[dbo].MovimientosTan AS t4 on t1.nrotrn = t4.nrotrn and t4.tiptrn = 4 and t4.volrec != 0
            LEFT JOIN [{short_db}].[dbo].Gasolineras t5 on t1.codgas = t5.cod
            LEFT JOIN [{short_db}].[dbo].DocumentosC t6 on t4.nrodoc = t6.nro and tip = 1 and t6.codgas = t1.codgas
            LEFT JOIN [{short_db}].[dbo].Proveedores t7 on t6.codopr = t7.cod
            LEFT JOIN (
                SELECT 
                    nro, codgas,
                    SUM(mto) / 100 AS monto,
                    SUM(can) AS cantidad,
                    SUM(pre) as precio
                FROM [{short_db}].[dbo].Documentos
                WHERE tip = 1
                GROUP BY codgas, nro
            ) t8 ON t8.nro = t4.nrodoc AND t8.codgas = t1.codgas
            WHERE 
                t1.tiptrn in (2)
                AND t1.fchtrn BETWEEN '{from_date}' AND '{until_date}'
                {proveedor_filter}
            ORDER BY fecha ASC, hora_formateada ASC
        """

        inner_query = inner_query.replace("'", "''")
        # JOIN con las DOS posibles facturas
        sql = f"""
            SELECT
                movimientos.*,
                -- Facturas Recibidas
                fr.Id as factura_recibida_id,
                fr.folio,fr.Fecha,fr.SubTotal,fr.Total,fr.EmisorNombre, fr.ReceptorNombre, fr.FechaTimbrado,fr.Destino,fr.RutaArchivo,fr.NombreArchivo,
                frc.Cantidad as factura_recibida_cantidad,frc.Descripcion,frc.ValorUnitario,frc.Importe,frc.NoIdentificacion,
                -- INFORMACIÓN DE LA RELACIÓN
                fmt.Id as relacion_id,
                fmt.TipoOperacion as tipo_operacion,
                fmt.FechaAsignacion as fecha_asignacion,
                fmt.UsuarioAsignacion as usuario_asignacion,
                fmt.Observaciones as observaciones,

                -- FACTURA PROVEEDOR ORIGINAL
                fmt.FacturaProveedorId as factura_proveedor_id,
                fmt.UUIDProveedor as uuid_proveedor,
                fmt.FolioProveedor as folio_proveedor,
                fmt.MontoProveedor as monto_proveedor,
                fmt.LitrosProveedor as litros_proveedor,
                fmt.PrecioProveedor as precio_proveedor,
                frProv.Fecha as fecha_factura_proveedor,
                frProv.EmisorNombre as emisor_factura_proveedor,
                frProv.Total as total_factura_proveedor,
                frProv.Destino as destino_factura_proveedor,
                frProv.Remision as remision_factura_proveedor,
                -- FACTURA PETROTAL (puede ser NULL)
                fmt.FacturaPetrotalId as factura_petrotal_id,
                fmt.UUIDPetrotal as uuid_petrotal,
                fmt.FolioPetrotal as folio_petrotal,
                fmt.MontoPetrotal as monto_petrotal,
                fmt.LitrosPetrotal as litros_petrotal,
                fmt.PrecioPetrotal as precio_petrotal,
                frPetro.Fecha as fecha_factura_petrotal,
                frPetro.EmisorNombre as emisor_factura_petrotal,
                frPetro.Total as total_factura_petrotal,
                frPetro.Destino as destino_factura_petrotal,
                frPetro.Remision as remision_factura_petrotal,

                -- FLAGS
                CASE 
                    WHEN fmt.Id IS NOT NULL THEN 1 
                    ELSE 0 
                END as tiene_facturas_asignadas,

                CASE 
                    WHEN fmt.TipoOperacion = 2 THEN 1
                    ELSE 0
                END as es_operacion_petrotal,
                -- CÁLCULOS AUTOMÁTICOS (diferencias, márgenes)
                CASE 
                    WHEN fmt.TipoOperacion = 2 
                    THEN fmt.PrecioPetrotal - fmt.PrecioProveedor
                    ELSE NULL
                END as diferencia_precio,
                CASE 
                    WHEN fmt.TipoOperacion = 2 AND fmt.PrecioProveedor > 0
                    THEN ((fmt.PrecioPetrotal - fmt.PrecioProveedor) / fmt.PrecioProveedor) * 100
                    ELSE NULL
                END as margen_porcentual
            FROM OPENQUERY([{linked_server}], '{inner_query}') as movimientos
            LEFT JOIN [TG].[dbo].[FacturasMovimientosTanques] fmt ON movimientos.nrotrn = fmt.nrotrn AND movimientos.codgas = fmt.codgas AND fmt.Activo = 1
            LEFT JOIN [TG].[dbo].[FacturasRecibidas] frProv ON fmt.FacturaProveedorId = frProv.Id
            LEFT JOIN [TG].[dbo].[FacturasRecibidas] frPetro ON fmt.FacturaPetrotalId = frPetro.Id
            LEFT JOIN [TG].dbo.FacturasRecibidas fr on movimientos.uuid_original = fr.UUID
            LEFT JOIN [TG].dbo.FacturasRecibidasConceptos frc on fr.Id = frc.FacturaId
        """
        
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            
            return [dict(zip(cols, row)) for row in rows]
            
        except Exception as e:
            print(f"Error ejecutando consulta para estación {codgas}: {e}")
            return []

    