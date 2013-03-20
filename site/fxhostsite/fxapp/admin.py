from django.contrib import admin
from fxapp.models import Customer, Strategy, StrategyExecution

admin.site.register(Customer)
admin.site.register(Strategy)
admin.site.register(StrategyExecution)
