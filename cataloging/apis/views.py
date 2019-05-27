from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse, HttpResponseNotFound
from django.views.decorators.http import require_http_methods
from apis.fileRead import read_csv
from apis.functions import *
from apis.redis import start_redis
import json

print("App is loading......")
read_csv()


@require_http_methods(["GET"])
def getRecentItem(request):
    date = request.GET.get('date','')
    result = get_recent_items(date)
    return HttpResponse(result,content_type="application/json")

@require_http_methods(["GET"])
def getBrandsCount(request):
    date = request.GET.get('date','')
    result = get_brand_count(date)
    return HttpResponse(result,content_type="application/json")

@require_http_methods(["GET"])
def getItemsByColor(request):
    color = request.GET.get('color','')
    result = get_latest_items_by_color(color)
    return HttpResponse(result,content_type="application/json")
