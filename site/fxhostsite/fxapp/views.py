from django.http import HttpResponse
#from django.template import Context, loader
from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required

from fxapp.models import Customer, Strategy, StrategyExecution, ExecutionEvent


@login_required
def index(request):
    # Outputs the last 5 customers to sign up
    customer_list = Customer.objects.order_by('-created')[:5]
    context = {'customer_list': customer_list}
    return render(request, 'fxapp/index.html', context)

def detail(request, customer_id):
    customer = get_object_or_404(Customer, pk=customer_id)
    strategy_list = customer.strategy_set.all()
    return render(request, 'fxapp/detail.html', {'customer': customer, 'strategy_list': strategy_list})

def strategy(request, strategy_id):
    strategy = get_object_or_404(Strategy, pk=strategy_id)
    run_list = strategy.strategyexecution_set.all()
    #return HttpResponse("%s runs available!" % len(run_list))
    return render(request, 'fxapp/strategy.html', {'strategy': strategy, 'runs':run_list})

def run(request, run_id):
    run = get_object_or_404(StrategyExecution, pk=run_id)
    return render(request, 'fxapp/run.html', {'run': run})
