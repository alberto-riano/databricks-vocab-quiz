from django.urls import path
from . import views

urlpatterns = [
    path("", views.quiz_page, name="quiz"),

    # VOCAB
    path("api/next/", views.next_question, name="next_question"),
    path("api/reset/", views.reset_game, name="reset_game"),
    path("api/check/", views.check_answer, name="check_answer"),

    # DATABRICKS
    path("api/dbx/next/", views.dbx_next, name="dbx_next"),
    path("api/dbx/check/", views.dbx_check, name="dbx_check"),
    path("api/dbx/reset/", views.dbx_reset, name="dbx_reset"),
]