from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.shortcuts import render


def login(request):
    if request.method == 'GET':
        next_page = request.GET.get('next')
        if next_page is None:
            raise Http404
        context = {'next_page':next_page,'valid':True}
        return render(request, 'login/index.html', context)

    username = request.POST['username']
    password = request.POST['password']
    next_page = request.POST.get('next')
    if next_page is None:
        raise Http404
    user = authenticate(username=username, password=password)
    if user is not None:
        if user.is_active:
            login(request, user)
            # Redirect to a success page.
            return HttpResponseRedirect(next_page)
        else:
            # Return a 'disabled account' error message
            context = {'username':username}
            return render(request, 'login/disabled_account.html', context)
    else:
        # Return an 'invalid login' error message.
        context = {'next_page':next_page,'valid':False}
        return render(request, 'login/index.html', context)
