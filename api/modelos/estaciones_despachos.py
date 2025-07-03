import pyodbc
from api.db_connections import CONTROLGASTG_CONN_STR
from datetime import datetime, timedelta


class EstacionDespachos:
    def __init__(self, conn_str: str = CONTROLGASTG_CONN_STR):
        self.conn_str = conn_str

    def estaciones(self):
        sql = """
        SELECT
        Servidor,BaseDatos,Codigo,Nombre
            FROM [TG].[dbo].[Estaciones]
        WHERE 
        Codigo in (2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40)
        ---- activa = 1 and Codigo != 0;
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

    def comparacion_despachos(self, servidor, basedatos, codigo):
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("EXEC dbo.ComparacionDespachosEstacion ?, ?, ?", (servidor, basedatos, codigo))
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [
                dict(zip(cols, row))
                for row in rows
            ]
        except Exception as e:
            print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
            return []

    # def comparacion_despachos(self, servidor, basedatos, codigo):
    #     """
    #     Ejecuta el query de comparación usando los parámetros dados.
    #     Devuelve lista de dicts con los resultados diarios de la semana.
    #     """
    #     # Armamos el query con los parámetros
    #     tsql = f"""
    #     DECLARE @estacion INT = {codigo};
    #     DECLARE @Servidor NVARCHAR(100) = '{servidor}';
    #     DECLARE @BaseDatos NVARCHAR(100) = '{basedatos}';

    #     DECLARE @dia_inicio DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));
    #     DECLARE @dia_fin DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE)); -- ayer
    #     DECLARE @fecha_actual DATE = @dia_inicio;
    #     DECLARE @fecha_inicial_int INT;

    #     IF OBJECT_ID('tempdb..#ComparacionDespachos') IS NOT NULL DROP TABLE #ComparacionDespachos;
    #     CREATE TABLE #ComparacionDespachos (
    #         Fecha DATE,
    #         TotalSG12 INT,
    #         TotalRemoto INT
    #     );

    #     WHILE @fecha_actual <= @dia_fin
    #     BEGIN
    #         SET @fecha_inicial_int = DATEDIFF(dd, 0, @fecha_actual) + 1;

    #         DECLARE @sql NVARCHAR(MAX);

    #         SET @sql = '
    #         INSERT INTO #ComparacionDespachos(Fecha, TotalSG12, TotalRemoto)
    #         SELECT 
    #             ''' + CONVERT(VARCHAR, @fecha_actual, 23) + ''' AS Fecha,
    #             (SELECT COUNT(*) 
    #              FROM [SG12].[dbo].[Despachos]
    #              WHERE fchtrn = ' + CAST(@fecha_inicial_int AS VARCHAR) + '
    #                AND codgas = ' + CAST(@estacion AS VARCHAR) + ') AS TotalSG12,
    #             (SELECT COUNT(*) 
    #              FROM [' + @Servidor + '].[' + @BaseDatos + '].[dbo].[Despachos]
    #              WHERE fchtrn = ' + CAST(@fecha_inicial_int AS VARCHAR) + '
    #                AND codgas = ' + CAST(@estacion AS VARCHAR) + ') AS TotalRemoto
    #         ';

    #         EXEC sp_executesql @sql;

    #         SET @fecha_actual = DATEADD(DAY, 1, @fecha_actual);
    #     END

    #     SELECT *, (TotalRemoto - TotalSG12) as diferencia,
    #               CAST((CAST(TotalSG12 AS FLOAT)*100.0)/NULLIF(TotalRemoto,0) AS DECIMAL(5,2)) as Porcentaje
    #     FROM #ComparacionDespachos
    #     ORDER BY Fecha DESC;
    #     """
    #     try:
    #         with pyodbc.connect(self.conn_str) as conn:
    #             cursor = conn.cursor()
    #             cursor.execute(tsql)
    #             cols = [col[0] for col in cursor.description]
    #             rows = cursor.fetchall()
    #             print(f"Resultados obtenidos para {codigo}: {len(rows)} filas")
    #         # Regresa una lista de dicts, agregando los datos de estación para referencia
    #         return [
    #             dict(zip(cols, row), **{"Estacion": codigo, "Servidor": servidor, "BaseDatos": basedatos})
    #             for row in rows
    #         ]
    #     except pyodbc.Error as e:
    #         print(f"Error ejecutando comparacion_despachos para {codigo}: {e}")
    #         return []

