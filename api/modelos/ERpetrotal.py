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
        

