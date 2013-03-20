from django.conf.urls import patterns, url
from fxapp import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^(?P<customer_id>\d+)/$', views.detail, name='detail'),
    url(r'^strategy/(?P<strategy_id>\d+)/$', views.strategy, name='strategy'),
    url(r'^run/(?P<run_id>\d+)/$', views.run, name='run')
    )
