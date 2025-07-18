import pyodbc

from .db_connections import ONEGOAL_CONN_STR

class OneGoal:
    def __init__(self):
        self.conn = pyodbc.connect(ONEGOAL_CONN_STR)
        self.cursor = self.conn.cursor()

    def get_bases_empresas(self):
       
        # Ahora busca las empresas activas
        query = "SELECT Nombre, db FROM [1G_TOTALGAS_MCP].dbo.cfg_empresa WHERE status = 1 AND c_mcp <> 1"
        rows = self.cursor.execute(query).fetchall()
        # Retorna lista de dicts
        empresas = [{'Nombre': row.Nombre, 'db': row.db} for row in rows]
        return empresas

    def get_edo_resultados_query(self, base, anio, nombre):
        query = f"""
            SELECT '{nombre}' As Empresa,
                [Nom Cen Cto] as CentroCosto,
                UPPER([Cat Cen Cto]) as CatCentroCosto,
                [num cta] as NoCuenta,
                categoria as Rubro,
                CASE WHEN [1g_vs_paq].nom_cta_paq IS NULL THEN [nom cta] ELSE [1g_vs_paq].nom_cta_paq END AS Concepto,
                SUM(CASE WHEN mes = 1 THEN monto*-1 ELSE 0 END ) AS Enero,
                SUM(CASE WHEN mes = 2 THEN monto*-1 ELSE 0 END ) AS Febrero,
                SUM(CASE WHEN mes = 3 THEN monto*-1 ELSE 0 END ) AS Marzo,
                SUM(CASE WHEN mes = 4 THEN monto*-1 ELSE 0 END ) AS Abril,
                SUM(CASE WHEN mes = 5 THEN monto*-1 ELSE 0 END ) AS Mayo,
                SUM(CASE WHEN mes = 6 THEN monto*-1 ELSE 0 END ) AS Junio,
                SUM(CASE WHEN mes = 7 THEN monto*-1 ELSE 0 END ) AS Julio,
                SUM(CASE WHEN mes = 8 THEN monto*-1 ELSE 0 END ) AS Agosto,
                SUM(CASE WHEN mes = 9 THEN monto*-1 ELSE 0 END ) AS Septiembre,
                SUM(CASE WHEN mes = 10 THEN monto*-1 ELSE 0 END ) AS Octubre,
                SUM(CASE WHEN mes = 11 THEN monto*-1 ELSE 0 END ) AS Noviembre,
                SUM(CASE WHEN mes = 12 THEN monto*-1 ELSE 0 END ) AS Diciembre
            FROM [{base}].[dbo].[vt_edo_res_x_cat_TGS] WITH (NOLOCK)
            LEFT OUTER JOIN vt_ctas_1g_vs_ctas_paq [1g_vs_paq]
                ON ([1g_vs_paq].num_cta_1g=[{base}].[dbo].[vt_edo_res_x_cat_TGS].[num cta])
            WHERE monto <> 0 AND [año] = ?
            GROUP BY año, categoria, [num cta], [1g_vs_paq].nom_cta_paq, [nom cta], [Cen Cto], [Nom Cen Cto], [Cat Cen Cto]
        """
        
        try:
            rows = self.cursor.execute(query, anio).fetchall() 
            cols = [column[0] for column in self.cursor.description]
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            print(f"Error: {e}")
            return []


def concentrado_og(year):
    onegoal = OneGoal()
    resultados = []

    NOMBRE_EMPRESA_MAP = {
        '1G_TOTALGAS': 'DIAZ GAS',
        '1G_TOTALGAS_EC': 'ESTACION CUSTODIA'
    }

    queries = []
    params = []
    for base in NOMBRE_EMPRESA_MAP.keys():
        nombre_visible = NOMBRE_EMPRESA_MAP[base]
        queries.append(f"""
            SELECT 
                '{nombre_visible}' AS Empresa,
                t.[Nom Cen Cto] AS CentroCosto,
                UPPER(t.[Cat Cen Cto]) AS CatCentroCosto,
                t.[num cta] COLLATE Modern_Spanish_CI_AS AS NoCuenta,
                t.categoria AS Rubro,
                CASE 
                WHEN [1g_vs_paq].nom_cta_paq IS NULL THEN
                    TRANSLATE(UPPER(t.[nom cta] COLLATE Traditional_Spanish_CI_AS),
                        'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÄËÏÖÜÃÕÇ',
                        'AEIOUAEIOUAEIOUAEIOUAOC'
                    )
                ELSE
                    TRANSLATE(UPPER([1g_vs_paq].nom_cta_paq),
                        'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÄËÏÖÜÃÕÇ',
                        'AEIOUAEIOUAEIOUAEIOUAOC'
                    )
            END AS Concepto,
                SUM(CASE WHEN t.mes = 1 THEN t.monto * -1 ELSE 0 END) AS Enero,
                SUM(CASE WHEN t.mes = 2 THEN t.monto * -1 ELSE 0 END) AS Febrero,
                SUM(CASE WHEN t.mes = 3 THEN t.monto * -1 ELSE 0 END) AS Marzo,
                SUM(CASE WHEN t.mes = 4 THEN t.monto * -1 ELSE 0 END) AS Abril,
                SUM(CASE WHEN t.mes = 5 THEN t.monto * -1 ELSE 0 END) AS Mayo,
                SUM(CASE WHEN t.mes = 6 THEN t.monto * -1 ELSE 0 END) AS Junio,
                SUM(CASE WHEN t.mes = 7 THEN t.monto * -1 ELSE 0 END) AS Julio,
                SUM(CASE WHEN t.mes = 8 THEN t.monto * -1 ELSE 0 END) AS Agosto,
                SUM(CASE WHEN t.mes = 9 THEN t.monto * -1 ELSE 0 END) AS Septiembre,
                SUM(CASE WHEN t.mes = 10 THEN t.monto * -1 ELSE 0 END) AS Octubre,
                SUM(CASE WHEN t.mes = 11 THEN t.monto * -1 ELSE 0 END) AS Noviembre,
                SUM(CASE WHEN t.mes = 12 THEN t.monto * -1 ELSE 0 END) AS Diciembre,
                'onegoal' AS origin
            FROM (
                SELECT 
                    cs.des AS categoria,
                    c.num_cta AS [num cta],
                    c.nom AS [nom cta],
                    SUM(ISNULL(v.cam_net, 0)) AS monto,
                    p.num_per AS mes,
                    p.año,
                    cen.des AS [Nom Cen Cto],
                    cen.codigo AS [Cen Cto],
                    cc.des AS [Cat Cen Cto]
                FROM 
                    [{base}].dbo.ctb_acu v WITH (NOLOCK)
                    FULL OUTER JOIN (
                        SELECT cp.id_per, cp.id_cta, cp.id_cen_cto, cp.mto
                        FROM [{base}].dbo.ctb_pre cp WITH (NOLOCK)
                        INNER JOIN [{base}].dbo.ctb_per_eje e WITH (NOLOCK) ON cp.id_per = e.id_per
                        INNER JOIN [{base}].dbo.pre_rev pr WITH (NOLOCK) ON cp.rev = pr.rev AND pr.id_eje = e.id_eje
                        WHERE pr.status = 1 AND cp.status = 1 AND pr.c_vigencia = 1 AND cp.id_tip_obj = 207
                    ) pr ON pr.id_cta = v.id_cta AND pr.id_per = v.id_per AND pr.id_cen_cto = v.id_cen_cto
                    INNER JOIN [{base}].dbo.ctb_cta c WITH (NOLOCK) ON ISNULL(v.id_cta, pr.id_cta) = c.id_cta
                    INNER JOIN [{base}].dbo.ctb_per_eje p WITH (NOLOCK) ON ISNULL(v.id_per, pr.id_per) = p.id_per
                    LEFT JOIN [{base}].dbo.ctb_cen_cto cen WITH (NOLOCK) ON cen.id_cen_cto = ISNULL(v.id_cen_cto, pr.id_cen_cto)
                    LEFT JOIN [{base}].dbo.vt_clas cc WITH (NOLOCK) ON cc.id_clas = cen.id_clas AND cc.id_clas_gral = 12
                    LEFT JOIN [{base}].dbo.cat_clas cs WITH (NOLOCK) ON cs.id_clas = c.id_clas1
                    LEFT JOIN [{base}].dbo.sis_tip s WITH (NOLOCK) ON c.id_tip = s.id_tip
                WHERE 
                    c.c_est = 0 AND
                    c.c_acu = 0 AND
                    p.num_per <> 13 AND
                    s.clase IN (4, 5, 6, 7)
                GROUP BY
                    cs.des, c.num_cta, c.nom, p.num_per, p.año, 
                    cen.des, cen.codigo, cc.des
            ) AS t
            LEFT JOIN [192.168.0.6].[TGV2].[dbo].[ctas_1g_vs_paq] AS [1g_vs_paq]
                ON t.[num cta] COLLATE Modern_Spanish_CI_AS = [1g_vs_paq].num_cta_1g
            WHERE t.año = ?
            and t.monto != 0 and  t.[num cta] != '40201000'
            GROUP BY
                t.año,
                t.categoria,
                t.[num cta] COLLATE Modern_Spanish_CI_AS,
                [1g_vs_paq].nom_cta_paq, 
                t.[nom cta],
                t.[Cen Cto],
                t.[Nom Cen Cto],
                t.[Cat Cen Cto]
        """)
        params.append(year)

    full_query = " UNION ALL ".join(queries)

    try:
        cursor = onegoal.conn.cursor()
        rows = cursor.execute(full_query, *params).fetchall()
        cols = [column[0] for column in cursor.description]
        resultados = [dict(zip(cols, row)) for row in rows]

        for r in resultados:
            if r.get('CentroCosto') == 'ES MUNICIPIO LIBRE':
                r['CentroCosto'] = 'MUNICIPIO LIBRE'
                r['CatCentroCosto'] = 'ESTACIONES'


        return resultados
    except Exception as e:
        print(f"Error: {e}")
        return []


# def is_og_available():
#     try:
#         onegoal = OneGoal()
#         cursor = onegoal.conn.cursor()
#         cursor.execute("SELECT 1")
#         return True
#     except Exception as e:
#         print(f"OG no disponible: {e}")
#         return False

