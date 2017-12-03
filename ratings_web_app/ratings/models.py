import os.path
from django.db import models


class Agency(models.Model):
    name = models.CharField(max_length=256, default='', unique=True)
    position = models.IntegerField(default=1)

    class Meta:
        ordering = ['position']

    def __str__(self):
        return '{} {}'.format(self.position, self.name)


class File(models.Model):
    path = models.CharField(max_length=1027, default='', unique=True)
    agency = models.ForeignKey(Agency, on_delete=models.CASCADE)
    lines_count = models.IntegerField(default=1)

    class Meta:
        ordering = ['agency__position', 'path']

    def __str__(self):
        return os.path.basename(self.path)
