# api/urls.py
from django.urls import path
from .views import concentrado_resultados_view
from .views import concentrado_anual_view
from .views import exportar_ingresos_excel
from .views import get_er_budget_view




urlpatterns = [
    # path('concentrado-og/', concentrado_og_view),
    path('concentrado-resultados/', concentrado_resultados_view),
    path('concentrado-anual/', concentrado_anual_view),
    path('exportar-ingresos/', exportar_ingresos_excel),
    path('get_er_budget/', get_er_budget_view),

]