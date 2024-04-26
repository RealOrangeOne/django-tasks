from django.urls import path

from . import views

urlpatterns = [
    path("meaning-of-life/", views.calculate_meaning_of_life, name="meaning-of-life")
]
