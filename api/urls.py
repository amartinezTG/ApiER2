# api/urls.py
from django.urls import path
from .views import (
    concentrado_resultados_view,
    concentrado_anual_view,
    exportar_ingresos_excel,
    get_er_budget_view,
    er_petrotal,
    er_petrotal_concept,
    gasto_petrotal
)
from .TG_php.views import (
    estacion_porcentaje,
    porcent_estacion_facturados_info,
    porcent_facturas_info,
    estacion_despachos_porcentaje,
    estacion_despachos_facturados_porcentaje,
    estacion_comparacion_series,
    estacion_documentos_compra,
    inventarios_distribuido,
    inventarios_detalles_distribuido,
    volumen_tanque,
    tanques_estacion,
    tanques_consolidado,
)
from .XmlCre.views import xmlCre


urlpatterns = [
    # path('concentrado-og/', concentrado_og_view),
    path('concentrado-resultados/', concentrado_resultados_view),
    path('concentrado-anual/', concentrado_anual_view),
    path('exportar-ingresos/', exportar_ingresos_excel),
    path('get_er_budget/', get_er_budget_view),
    path('er_petrotal/', er_petrotal),
    path('er_petrotal_concept/', er_petrotal_concept),
    # Rutas para TG_php
    path('estacion_porcentaje/', estacion_porcentaje),
    path('porcent_estacion_facturados_info/', porcent_estacion_facturados_info),
    path('porcent_facturas_info/', porcent_facturas_info),
    path('gasto_petrotal/', gasto_petrotal),
    path('estacion_despachos_porcentaje/', estacion_despachos_porcentaje),
    path('estacion_despachos_facturados_porcentaje/', estacion_despachos_facturados_porcentaje),
    path('estacion_comparacion_series/', estacion_comparacion_series),
    #documentos
    path('estacion_documentos_compra/', estacion_documentos_compra),
    # Rutas para XmlCre
    path('xmlCre/', xmlCre),
    path('inventarios/', inventarios_distribuido),
    path('inventarios/detalles/',  inventarios_detalles_distribuido),
    path('tanques/estacion/', tanques_estacion),
    path('tanques/volumen/', volumen_tanque),
    path('tanques/consolidado/', tanques_consolidado),


]