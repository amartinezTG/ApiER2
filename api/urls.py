# api/urls.py
from django.urls import path
from .views import (
    concentrado_resultados_view,
    concentrado_anual_view,
    exportar_ingresos_excel,
    get_er_budget_view,
    er_petrotal,
    er_petrotal_concept
)
from .TG_php.views import (
    estacion_porcentaje,
    porcent_estacion_facturados_info,
    porcent_facturas_info
)


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

]