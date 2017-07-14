#!/usr/bin/env python
import os
from datetime import date, datetime
import calendar
import re
import time
import sys

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

def get_starting_day_of_dekad(year,month,dekad):

    first_dekads = range( 1, 37, 3)
    second_dekads = range ( 2 , 37 , 3 )
    thirds_dekads = range ( 3 , 37 , 3 )

    month_year =  ( str(month),str(year) )
    final_date = None
    if dekad in first_dekads:
        final_date = ('01',) + month_year + ('12:00:00',)
    elif dekad in second_dekads:
        final_date = ('11', ) + month_year + ('12:00:00',)
    elif dekad in thirds_dekads:
        final_date =  ('21',) +month_year + ('12:00:00',)

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

def metadata_generator(gee_file):

    gee_asset = gee_file.split ( '.' )[0]
    level = re.split ( '(\d.*)' , gee_file.split ( "_" )[0])[1]
    area = 'AfNE'

    initial_date = datetime.strptime('1970-1-1',"%Y-%m-%d")
    date_part = gee_file.split ( '_' )[2]
    if len(date_part) == 4:
        year = int ( '20' + gee_file.split ( '_' )[2][0:2] )
        dekad = int(gee_file.split ( '_' )[2][2:4])
        number_of_days = get_days_in_dekad(year,dekad)
        month = get_month_of_dekad ( dekad)
        start_date = datetime.strptime(get_starting_day_of_dekad(year,month,dekad),"%d-%m-%Y-%H:%M:%S")
        #unix_time_auto = time.mktime ( start_date.timetuple ())
        unix_time_manual = (start_date-initial_date).total_seconds() * 1000
        #metadata dictionary is necessary??? YES
        return {'id_no':gee_file,'system:time_start':unix_time_manual,'days_in_dk':number_of_days}
        #return [gee_asset,level ,area, unix_time_manual, number_of_days]
    elif len(date_part) == 6:
        year = int ( '20' + gee_file.split ( '_' )[2][0:2] )
        month =  int(gee_file.split ( '_' )[2][2:4])
        day =  int(gee_file.split ( '_' )[2][4:6])
        s = str(day) + "-" + str(month) + "-" + str(year) + "-" + '12:00:00'
        start_date = datetime.strptime (s , "%d-%m-%Y-%H:%M:%S" )
        unix_time_manual = (start_date - initial_date).total_seconds () * 1000
        #unix_time_auto = time.mktime ( start_date.timetuple () )
        # metadata dictionary is necessary?? YES
        return {'id_no':gee_file, 'system:time_start': unix_time_manual}
        #return [gee_asset,level,area,unix_time_manual]

def metadata_pre_processing(path_test):

    if os.path.isfile ( path_test ):
        file_only = os.path.split ( path_test )[1]
        level = re.split ( '(\d.*)' , file_only.split ( "_" )[0])
        gee_file = file_only.split ( "." )[0]
        metadata = metadata_generator ( gee_file )
        metadata['level'] = level[1]
        metadata['area'] = 'AfNE'
        return metadata
    elif os.path.isdir(path_test):
        new_files = []
        for (dirpath , dirnames , filenames) in os.walk(path_test):
            new_files.extend ( filenames )
            break
        lista_metadati = []
        for each_file in new_files:
            level = re.split ( '(\d.*)', each_file.split ( "_" )[0])
            gee_file = each_file.split(".")[0]
            metadata = metadata_generator(gee_file)
            metadata['level'] = level[1]
            metadata['area'] = 'WANE'
            lista_metadati.append(metadata)
        return lista_metadati

def no_data_assessment(path_test):

    file_only = os.path.split ( path_test )[1]
    level,data_component_code = file_only.split ( '_' )[0], file_only.split ( '_' )[1]

    if level == 'L1':
        if data_component_code == 'AET':
            no_data = 255
        elif data_component_code == 'NPP':
            no_data = -9999
        elif data_component_code == 'AGBP':
            no_data = -9999
        elif data_component_code == 'RET':
            no_data = 255
        elif data_component_code == 'PCP':
            no_data =  -9999
        elif data_component_code == 'LCC':
            no_data =  255
        elif data_component_code == 'TFRAC':
            no_data = 255
        else:
            no_data = None
    elif level == 'L2':
        if data_component_code == 'AET':
            no_data = 255
        elif data_component_code == 'NPP':
            no_data = -9999
        elif data_component_code == 'AGBP':
            no_data = -9999
        elif data_component_code == 'RET':
            no_data = 255
        elif data_component_code == 'PCP':
            no_data =  -9999
        elif data_component_code == 'LCC':
            no_data =  255
        elif data_component_code == 'TFRAC':
            no_data = 255
        else:
            no_data = None
    else:
        no_data = None
    return no_data

def main():

    #path_or_file = '../wapor_algorithms/image_test/L1_NPP_0933.tif'
    path_or_file = '../wapor_algorithms/image_test'

    no_data_assessment(path_or_file)
    print metadata_pre_processing(path_or_file)

if __name__ == "__main__":
    main ()