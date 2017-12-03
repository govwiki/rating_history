import os
from django.views.generic import ListView
from .models import File


class IndexView(ListView):
    model = File
    template_name = 'files_list.html'
    context_object_name = 'files'
