from django.db import models

class Customer(models.Model):
    username = models.CharField(max_length=64)
    created = models.DateTimeField('date created')

    def __unicode__(self):
        return self.username

class Strategy(models.Model):
    name = models.CharField(max_length=128)
    customer = models.ForeignKey(Customer)

    def __unicode__(self):
        return self.name

