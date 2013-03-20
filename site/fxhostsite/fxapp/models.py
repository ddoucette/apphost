from django.db import models


class Customer(models.Model):
    username = models.CharField(max_length=64)
    created = models.DateTimeField('date created')

    def __unicode__(self):
        return self.username

class Strategy(models.Model):
    name = models.CharField(max_length=128)
    customer = models.ForeignKey(Customer)
    created = models.DateTimeField('date created')

    def __unicode__(self):
        return self.name

class StrategyExecution(models.Model):
    time = models.DateTimeField('start time')
    strategy = models.ForeignKey(Strategy)
    complete = models.BooleanField()

    def __unicode__(self):
        return ":".join([self.strategy.name, str(self.time)])

class ExecutionEvent(models.Model):
    event_type = models.CharField(max_length=32)
    event_name = models.CharField(max_length=32)
    event_string = models.CharField(max_length=256)
    strat_exec = models.ForeignKey(StrategyExecution)

    def __unicode__(self):
        return str(self.strat_exec) + ":" + self.event_name
