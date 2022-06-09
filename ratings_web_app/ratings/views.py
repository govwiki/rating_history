import os

from django.core.exceptions import ImproperlyConfigured
from django.db.models import QuerySet
from django.views.generic import ListView
from .models import File


class IndexView(ListView):
    model = File
    template_name = 'files_list.html'
    context_object_name = 'files'




