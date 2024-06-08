from django.contrib import admin
from django.urls import path

from . import views

urlpatterns = [
    path("meaning-of-life/", views.calculate_meaning_of_life, name="meaning-of-life"),
    path("result/<str:result_id>", views.get_task_result, name="result"),
    path(
        "meaning-of-life-async/",
        views.calculate_meaning_of_life_async,
        name="meaning-of-life-async",
    ),
    path("admin/", admin.site.urls),
]
