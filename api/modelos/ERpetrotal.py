import pyodbc
from api.db_connections import CONTROLGAS_CONN_STR

class Erpetrotal:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str

    def get_petrotal_utilidad(self, date: str = '2025-01-01'):
        sql = """
        select sum(valor) as utilidad from [TGV2].[dbo].[ERPetrotal] where [fecha]  = ?
        """
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, date)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []
        
    def utilidad_petrotal_estacion(self, date,fecha_ultimo_dia):
        sql = """
        WITH Totales AS (
            SELECT 
                SUM(CASE WHEN combustible = 'DIESEL' THEN utilidad_perdida ELSE 0 END) AS TotalDiesel,
                SUM(CASE WHEN combustible = 'PREMIUM' THEN utilidad_perdida ELSE 0 END) AS TotalPremium,
                SUM(CASE WHEN combustible = 'REGULAR' THEN utilidad_perdida ELSE 0 END) AS TotalRegular
            FROM [TGV2].[dbo].[ERComprasPetrotal]
            WHERE indicador_1 = 'propias'   and [fecha] between ? and ?
        ),
        utilidad_real_valor as (

            SELECT
                SUM(CASE WHEN [cuenta] IN ('Ingresos Gasolina Regular', 'Devs decsuecuentos-bonifica', 'Costo Gasolina Regular', 'Costo Almacenaje Regular')
                        THEN valor ELSE 0 END) AS utilidad_real_regular,
                SUM(CASE WHEN [cuenta] IN ('Ingresos Gasolina Premium', 'Costo Gasolina Premium', 'Costo Almacenaje premium', 'Flete Magna')
                        THEN valor ELSE 0 END) AS utilidad_real_premium,
                SUM(CASE WHEN [cuenta] IN ('INGRESOS DIESEL', 'COSTO DIESEL', 'Costo Almacenaje Diesel')
                        THEN valor ELSE 0 END) AS utilidad_real_diesel
            FROM [TGV2].[dbo].[ERPetrotal]
            WHERE [fecha] = ?
        )
        ,
        PorEstacion AS (
            SELECT 
                estacion,
                num_estacion AS [Etiquetas de fila],
                SUM(CASE WHEN combustible = 'DIESEL' THEN utilidad_perdida ELSE 0 END) AS DIESEL,
                SUM(CASE WHEN combustible = 'PREMIUM' THEN utilidad_perdida ELSE 0 END) AS PREMIUM,
                SUM(CASE WHEN combustible = 'REGULAR' THEN utilidad_perdida ELSE 0 END) AS REGULAR
            FROM [TGV2].[dbo].[ERComprasPetrotal]
            WHERE indicador_1 = 'propias'   and [fecha] between ? and ?
            GROUP BY estacion, num_estacion 
        ),
        porcentages_tabla as(
        SELECT 
            e.estacion,
            e.[Etiquetas de fila],
            e.DIESEL,
            e.PREMIUM,
            e.REGULAR,
            -- Porcentaje de cada estación respecto al total general del combustible
            CASE WHEN t.TotalDiesel > 0 THEN e.DIESEL * 100 / t.TotalDiesel ELSE 0 END AS diesel_porcentaje,
            CASE WHEN t.TotalPremium > 0 THEN e.PREMIUM * 100 / t.TotalPremium ELSE 0 END AS premium_porcentaje,
            CASE WHEN t.TotalRegular > 0 THEN e.REGULAR * 100 / t.TotalRegular ELSE 0 END AS regular_porcentaje
        FROM PorEstacion e
        CROSS JOIN Totales t
        )
        select 
        t1.*,
        t1.diesel_porcentaje * URV.utilidad_real_diesel / 100 as diesel_utilidad,
        t1.premium_porcentaje * URV.utilidad_real_premium / 100 as premium_utilidad,
        t1.regular_porcentaje * URV.utilidad_real_regular / 100 as regular_utilidad
        from porcentages_tabla t1
        CROSS JOIN utilidad_real_valor URV
        """
        params = [date,fecha_ultimo_dia,date,date,fecha_ultimo_dia,]
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql,params)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []
    
    def er_petrotal_concept(self, date):
        sql = """
        SELECT TOP (1000) [id]
            ,[rubro]
            ,[cuenta]
            ,isnull([valor],0) as valor
            ,[fecha]
            ,[fecha_creacion]
        FROM [TGV2].[dbo].[ERPetrotal]
        where fecha = ?
        """
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql,date)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []

    def get_petrotal_utilidad(self, date: str = '2025-01-01'):
        sql = """
        select sum(valor) as utilidad from [TGV2].[dbo].[ERPetrotal] where [fecha]  = ?
        """
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, date)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []
        
    def gasto_petrotal_estacion(self, date,fecha_ultimo_dia):
        sql = """
            WITH  PorEstacion AS (
                SELECT 
                   CASE
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'VILLA' 
                        THEN 'Villa Ahumada'
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'KILOMETRO 30' 
                        THEN 'Travel Centrer'
                    ELSE
                        -- Remueve el número y espacio inicial usando PATINDEX
                        LTRIM(SUBSTRING(
                            LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))),
                            PATINDEX('%[A-Z]%', LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))),
                            LEN(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))))
                        ))
                END AS estacion,
                    num_estacion AS [Etiquetas de fila],
                    SUM(litros) as litros
                FROM [TGV2].[dbo].[ERComprasPetrotal]
                WHERE indicador_1 = 'propias'   and [fecha] between ? and ?
                GROUP BY estacion, num_estacion 
                    ),
            total_listros_petrotal as (
            SELECT 
            SUM(litros) as [total_litros]
            FROM [TGV2].[dbo].[ERComprasPetrotal]
            WHERE indicador_1 = 'propias'   and [fecha] between ? and ?
            ),
            gasto_total as(
                SELECT gasto   FROM [TGV2].[dbo].[ERBalancePetrotal] where fecha = ?
            )
            select
            pe.*,
            (gt.gasto/tlp.total_litros )*pe.litros as flete
            from PorEstacion pe
            CROSS JOIN gasto_total gt
            CROSS JOIN total_listros_petrotal tlp
            """
        params = [date, fecha_ultimo_dia, date, fecha_ultimo_dia, date]
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []
    

    def concept_costo_petrotal(self, year):
        sql = """
        -- Generar todos los meses del año 2025
        WITH Meses AS (
            SELECT 
                1 AS Mes,
                DATEFROMPARTS(?, 1, 1) AS FechaInicio,
                EOMONTH(DATEFROMPARTS(?, 1, 1)) AS FechaFin
            UNION ALL
            SELECT 
                Mes + 1,
                DATEFROMPARTS(?, Mes + 1, 1),
                EOMONTH(DATEFROMPARTS(?, Mes + 1, 1))
            FROM Meses
            WHERE Mes < 12
        ),

        Totales AS (
            SELECT 
                m.Mes,
                SUM(CASE WHEN c.combustible = 'DIESEL' THEN c.utilidad_perdida ELSE 0 END) AS TotalDiesel,
                SUM(CASE WHEN c.combustible = 'PREMIUM' THEN c.utilidad_perdida ELSE 0 END) AS TotalPremium,
                SUM(CASE WHEN c.combustible = 'REGULAR' THEN c.utilidad_perdida ELSE 0 END) AS TotalRegular
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERComprasPetrotal] c
                ON c.indicador_1 = 'propias'
                AND c.[fecha] BETWEEN m.FechaInicio AND m.FechaFin
            GROUP BY m.Mes
        ),
        utilidad_real_valor AS (
            SELECT
                m.Mes,
                SUM(CASE WHEN [cuenta] IN ('Ingresos Gasolina Regular', 'Devs decsuecuentos-bonifica', 'Costo Gasolina Regular', 'Costo Almacenaje Regular')
                        THEN valor ELSE 0 END) AS utilidad_real_regular,
                SUM(CASE WHEN [cuenta] IN ('Ingresos Gasolina Premium', 'Costo Gasolina Premium', 'Costo Almacenaje premium', 'Flete Magna')
                        THEN valor ELSE 0 END) AS utilidad_real_premium,
                SUM(CASE WHEN [cuenta] IN ('INGRESOS DIESEL', 'COSTO DIESEL', 'Costo Almacenaje Diesel')
                        THEN valor ELSE 0 END) AS utilidad_real_diesel
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERPetrotal] p
                ON p.[fecha] = m.FechaInicio
            GROUP BY m.Mes
        ),

        PorEstacion AS (
            SELECT 
                m.Mes,
                CASE
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'VILLA' 
                        THEN 'Villa Ahumada'
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'KILOMETRO 30' 
                        THEN 'Travel Centrer'
                    ELSE
                        -- Remueve el número y espacio inicial usando PATINDEX
                        LTRIM(SUBSTRING(
                            LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))),
                            PATINDEX('%[A-Z]%', LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))),
                            LEN(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))))
                        ))
                END AS estacion,
                c.num_estacion AS [Etiquetas de fila],
                SUM(CASE WHEN c.combustible = 'DIESEL' THEN c.utilidad_perdida ELSE 0 END) AS DIESEL,
                SUM(CASE WHEN c.combustible = 'PREMIUM' THEN c.utilidad_perdida ELSE 0 END) AS PREMIUM,
                SUM(CASE WHEN c.combustible = 'REGULAR' THEN c.utilidad_perdida ELSE 0 END) AS REGULAR
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERComprasPetrotal] c
                ON c.indicador_1 = 'propias'
                AND c.[fecha] BETWEEN m.FechaInicio AND m.FechaFin
            GROUP BY m.Mes, c.estacion, c.num_estacion
        ),

        porcentages_tabla AS (
            SELECT 
                e.Mes,
                e.estacion,
                e.[Etiquetas de fila],
                e.DIESEL,
                e.PREMIUM,
                e.REGULAR,
                -- Porcentaje de cada estación respecto al total general del combustible
                CASE WHEN t.TotalDiesel > 0 THEN e.DIESEL * 100.0 / t.TotalDiesel ELSE 0 END AS diesel_porcentaje,
                CASE WHEN t.TotalPremium > 0 THEN e.PREMIUM * 100.0 / t.TotalPremium ELSE 0 END AS premium_porcentaje,
                CASE WHEN t.TotalRegular > 0 THEN e.REGULAR * 100.0 / t.TotalRegular ELSE 0 END AS regular_porcentaje
            FROM PorEstacion e
            INNER JOIN Totales t ON e.Mes = t.Mes
        ),
        semi_tabla as(

            SELECT 
                pt.Mes,
                DATENAME(month, DATEFROMPARTS(2025, pt.Mes, 1)) AS NombreMes,
                pt.estacion,
                pt.[Etiquetas de fila],
                pt.DIESEL,
                pt.PREMIUM,
                pt.REGULAR,
                pt.diesel_porcentaje,
                pt.premium_porcentaje,
                pt.regular_porcentaje,
                pt.diesel_porcentaje * urv.utilidad_real_diesel / 100 AS diesel_utilidad,
                pt.premium_porcentaje * urv.utilidad_real_premium / 100 AS premium_utilidad,
                pt.regular_porcentaje * urv.utilidad_real_regular / 100 AS regular_utilidad
            FROM porcentages_tabla pt
            INNER JOIN utilidad_real_valor urv ON pt.Mes = urv.Mes
        ),
        unpivot_tabla AS (
            SELECT
                'DIAZ GAS' AS Empresa,
                estacion AS [CentroCosto],
                'estaciones' AS [CatCentroCosto],
                '501010010' AS [NoCuenta],
                'B - COSTO DE VENTA' AS [Rubro],
                NombreMes,
                Mes,
                v.Concepto,
                v.Valor
            FROM semi_tabla
            CROSS APPLY (
                VALUES
                    ('COSTO MAGNA', regular_utilidad),
                    ('COSTO PREMIUM', premium_utilidad),
                    ('COSTO DIESEL', diesel_utilidad)
            ) v(Concepto, Valor)
            WHERE estacion IS NOT NULL
        )

        SELECT
            Empresa,
            [CentroCosto],
            [CatCentroCosto],
            [NoCuenta],
            [Rubro],
            [Concepto],
            SUM(CASE WHEN Mes = 1 THEN Valor ELSE 0 END) AS [Enero],
            SUM(CASE WHEN Mes = 2 THEN Valor ELSE 0 END) AS [Febrero],
            SUM(CASE WHEN Mes = 3 THEN Valor ELSE 0 END) AS [Marzo],
            SUM(CASE WHEN Mes = 4 THEN Valor ELSE 0 END) AS [Abril],
            SUM(CASE WHEN Mes = 5 THEN Valor ELSE 0 END) AS [Mayo],
            SUM(CASE WHEN Mes = 6 THEN Valor ELSE 0 END) AS [Junio],
            SUM(CASE WHEN Mes = 7 THEN Valor ELSE 0 END) AS [Julio],
            SUM(CASE WHEN Mes = 8 THEN Valor ELSE 0 END) AS [Agosto],
            SUM(CASE WHEN Mes = 9 THEN Valor ELSE 0 END) AS [Septiembre],
            SUM(CASE WHEN Mes = 10 THEN Valor ELSE 0 END) AS [Octubre],
            SUM(CASE WHEN Mes = 11 THEN Valor ELSE 0 END) AS [Noviembre],
            SUM(CASE WHEN Mes = 12 THEN Valor ELSE 0 END) AS [Diciembre],
            'petrotal' AS origin
        FROM unpivot_tabla
        GROUP BY Empresa, [CentroCosto], [CatCentroCosto], [NoCuenta], [Rubro], [Concepto]
        ORDER BY [CentroCosto], [Concepto]
        """
        params = [year, year, year, year]
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql,params)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []

    def concept_flete_petrotal(self, year):
        sql = """
        -- Generar todos los meses del año 2025
        WITH Meses AS (
            SELECT 
                1 AS Mes,
                DATEFROMPARTS(?, 1, 1) AS FechaInicio,
                EOMONTH(DATEFROMPARTS(?, 1, 1)) AS FechaFin
            UNION ALL
            SELECT 
                Mes + 1,
                DATEFROMPARTS(?, Mes + 1, 1),
                EOMONTH(DATEFROMPARTS(?, Mes + 1, 1))
            FROM Meses
            WHERE Mes < 12
        ),
        LitrosPorEstacion AS (
            SELECT 
                m.Mes,
               CASE
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'VILLA' 
                        THEN 'Villa Ahumada'
                    WHEN UPPER(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))) = 'KILOMETRO 30' 
                        THEN 'TRAVEL CENTER'
                    ELSE
                        -- Remueve el número y espacio inicial usando PATINDEX
                        LTRIM(SUBSTRING(
                            LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))),
                            PATINDEX('%[A-Z]%', LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion)))),
                            LEN(LTRIM(SUBSTRING(c.estacion, CHARINDEX('-', c.estacion) + 1, LEN(c.estacion))))
                        ))
                END AS estacion,
                SUM(c.litros) as litros
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERComprasPetrotal] c
                ON c.indicador_1 = 'propias'
                AND c.[fecha] BETWEEN m.FechaInicio AND m.FechaFin
            GROUP BY m.Mes, c.estacion
        ),
        TotalLitrosMes AS (
            SELECT 
                m.Mes,
                SUM(c.litros) AS total_litros
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERComprasPetrotal] c
                ON c.indicador_1 = 'propias'
                AND c.[fecha] BETWEEN m.FechaInicio AND m.FechaFin
            GROUP BY m.Mes
        ),
        GastoMensual AS (
            SELECT 
                m.Mes,
                b.gasto
            FROM Meses m
            LEFT JOIN [TGV2].[dbo].[ERBalancePetrotal] b
                ON b.fecha = m.FechaInicio
        ),
        FleteMensual AS (
            SELECT
                'DIAZ GAS' AS Empresa,
                lpe.estacion AS CentroCosto,
                'estaciones' AS CatCentroCosto,
                '60301000' AS NoCuenta,
                'B - COSTO DE VENTA' AS Rubro,
                'FLETE PETROTAL' AS Concepto,
                lpe.Mes,
                CASE 
                    WHEN tlm.total_litros > 0 THEN (gm.gasto / tlm.total_litros) * lpe.litros
                    ELSE 0
                END AS Valor
            FROM LitrosPorEstacion lpe
            LEFT JOIN TotalLitrosMes tlm ON lpe.Mes = tlm.Mes
            LEFT JOIN GastoMensual gm ON lpe.Mes = gm.Mes
        )

        SELECT
            Empresa,
            CentroCosto,
            CatCentroCosto,
            NoCuenta,
            Rubro,
            Concepto,
            SUM(CASE WHEN Mes = 1 THEN Valor ELSE 0 END) AS [Enero],
            SUM(CASE WHEN Mes = 2 THEN Valor ELSE 0 END) AS [Febrero],
            SUM(CASE WHEN Mes = 3 THEN Valor ELSE 0 END) AS [Marzo],
            SUM(CASE WHEN Mes = 4 THEN Valor ELSE 0 END) AS [Abril],
            SUM(CASE WHEN Mes = 5 THEN Valor ELSE 0 END) AS [Mayo],
            SUM(CASE WHEN Mes = 6 THEN Valor ELSE 0 END) AS [Junio],
            SUM(CASE WHEN Mes = 7 THEN Valor ELSE 0 END) AS [Julio],
            SUM(CASE WHEN Mes = 8 THEN Valor ELSE 0 END) AS [Agosto],
            SUM(CASE WHEN Mes = 9 THEN Valor ELSE 0 END) AS [Septiembre],
            SUM(CASE WHEN Mes = 10 THEN Valor ELSE 0 END) AS [Octubre],
            SUM(CASE WHEN Mes = 11 THEN Valor ELSE 0 END) AS [Noviembre],
            SUM(CASE WHEN Mes = 12 THEN Valor ELSE 0 END) AS [Diciembre],
            'petrotal' AS origin
        FROM FleteMensual
        GROUP BY Empresa, CentroCosto, CatCentroCosto, NoCuenta, Rubro, Concepto
        ORDER BY CentroCosto, Concepto
        OPTION (MAXRECURSION 100)
        """
        params = [year, year, year, year]
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql,params)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []
