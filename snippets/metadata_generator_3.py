#!/usr/bin/env python
import os
from datetime import date, datetime
import calendar
import re
import time
import sys
import operator
import csv

def get_days_in_dekad(year , dekade):
    dekads_eleven = [3 , 8 , 15 , 20 , 24 ,30 , 36]
    if dekade == 6:
        days = calendar.monthrange ( year , 2 )[1]-20
    elif dekade in dekads_eleven:
        days = 11
    else:
        days = 10
    return days


def get_month_of_dekad(dekad):

    if dekad<3:
        month_dekad = 1
    elif dekad >3 and dekad <= 6:
        month_dekad = 2
    elif dekad > 6 and dekad <= 9:
        month_dekad = 3
    elif dekad > 9 and dekad <= 12:
        month_dekad = 4
    elif dekad > 12 and dekad <= 15:
        month_dekad = 5
    elif dekad > 15 and dekad <= 18:
        month_dekad = 6
    elif dekad > 18 and dekad <= 21:
        month_dekad = 7
    elif dekad > 21 and dekad <= 24:
        month_dekad = 8
    elif dekad > 24 and dekad <= 27:
        month_dekad = 9
    elif dekad > 27 and dekad <= 30:
        month_dekad = 10
    elif dekad > 30 and dekad <= 33:
        month_dekad = 11
    else:
        month_dekad = 12

    return month_dekad


def get_dekad_within_month(dekad):

    dekad_within_month = dekad/3

    return dekad_within_month


def get_starting_day_of_dekad(year, month, dekad):

    first_dekads = range( 1, 37, 3)
    second_dekads = range ( 2 , 37 , 3 )
    thirds_dekads = range ( 3 , 37 , 3 )
    hour = '00:00:00'

    month_year =  ( str(month),str(year) )
    final_date = None
    if dekad in first_dekads:
        final_date = ('01',) + month_year + (hour,)
    elif dekad in second_dekads:
        final_date = ('11', ) + month_year + (hour,)
    elif dekad in thirds_dekads:
        final_date = ('21',) +month_year + (hour,)

    starting_day = '-'.join (final_date)

    return  starting_day


def check_dekad(date):

    if date.day < 11:
        dekad = 10
    elif date.day > 10 and date.day < 21:
        dekad = 20
    else:
        dekad = calendar.monthrange(date.year, date.month)[1]

    new_date = datetime(date.year, date.month, dekad)

    return new_date


def day2dekad(day):

    if day < 11:
        dekad = 1
    elif day > 10 and day < 21:
        dekad = 2
    else:
        dekad = 3

    return dekad


def get_dekad_period(dates):

    period = []

    for dat in dates:
        d = check_dekad(dat)
        dekad = day2dekad(d.day)
        per = dekad + ((d.month - 1) * 3)
        period.append(per)

    return period


def metadata_generator(gee_file, seasonal=False, area='AfNE'):

    initial_date = datetime.strptime('01-01-1970', "%d-%m-%Y")

    id_no = gee_file.split(".")[0]
    level = re.split('(\d.*)', gee_file.split("_")[0])[1]

    date_format = "%d-%m-%Y-%H:%M:%S"

    if seasonal:
        date_part = id_no.split('_')[-2]
        season = id_no[3:4]
        season_time = id_no.split('_')[-1]
        year = int('20' + date_part[0:2])
        area_part = gee_file.split('_')[1]
        start_date = datetime.strptime("01-01-" + str(year) + "-12:00:00", date_format)
        unix_time_manual = (start_date-initial_date).total_seconds() * 1000
        metadata = {'id_no': id_no, 'season': season, "area": area_part, 'system:time_start': unix_time_manual,
                'year': year, "season_time": season_time, 'level': level}
    else:
        date_part = id_no.split('_')[-1]
        if len(date_part) == 4:
            year = int('20' + date_part[0:2])
            dekad = int(date_part[2:4])
            number_of_days = get_days_in_dekad(year, dekad)
            month = get_month_of_dekad ( dekad)
            start_date = datetime.strptime(get_starting_day_of_dekad(year, month, dekad), date_format)
            unix_time_manual = (start_date-initial_date).total_seconds() * 1000
            metadata = {'id_no': id_no, 'system:time_start': unix_time_manual, 'days_in_dk':
                      number_of_days,'level': level, 'area': area}
        elif len(date_part) == 6:
            year = int('20' + gee_file.split('_')[2][0:2])
            month = int(gee_file.split( '_' )[2][2:4])
            day = int(gee_file.split('_')[2][4:6])
            s = str(day) + "-" + str(month) + "-" + str(year) + "-" + '12:00:00'
            start_date = datetime.strptime(s, date_format)
            unix_time_manual = (start_date - initial_date).total_seconds() * 1000
            metadata = {'id_no': id_no, 'system:time_start': unix_time_manual, 'level': level, 'area': area}

    return metadata

def no_data_assessment(data_component_code):


    if 'AET' in data_component_code :
        no_data = 255
    elif 'NPP' in data_component_code:
        no_data = -9999
    elif 'AGBP' in data_component_code :
        no_data = -9999
    elif 'RET' in data_component_code:
        no_data = 255
    elif 'PCP' in data_component_code:
        no_data =  -9999
    elif 'LCC' in data_component_code:
        no_data =  255
    elif 'TFRAC' in data_component_code :
        no_data = 255
    elif 'PHE' in data_component_code:
            no_data = 255
    else:
        no_data = None

    return no_data


def main():

    #path_or_file = r'Z:\L2\AET\L2_WANE_AET_0910.tif'
    #path_or_file = r'Z:\L2\AET'
    #path_or_file = r'Z:\L2\PHE'

    #path_or_file = '../wapor_algorithms/image_test/L2_WANE_AET_0910.tif'
    #path_or_file = '../wapor_algorithms/image_test/L2_EANE_PHE_09s1_e.tif'
    path_or_file = '../wapor_algorithms/image_test/'

    new_files = []
    if os.path.isfile(path_or_file):
        new_files.append(os.path.basename(path_or_file))
    elif os.path.isdir(path_or_file):
        for (dirpath , dirnames , filenames) in os.walk ( path_or_file ):
            new_files.extend(filenames)
            break

    list_metadata = []
    for file_name in new_files:
        no_data = no_data_assessment(file_name)
        if "L1" in file_name:
            metadata = metadata_generator(file_name)
            list_metadata.append ( metadata )
        if "L2" in file_name:
            if "s1" in file_name or "s2" in file_name:
                metadata = metadata_generator (file_name, seasonal=True)
                list_metadata.append(metadata)
            else:
                metadata = metadata_generator(file_name)
                list_metadata.append(metadata)
        print(no_data, metadata)

    print list_metadata

if __name__ == "__main__":
    main ()