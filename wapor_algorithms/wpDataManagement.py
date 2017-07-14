#!/usr/bin/env python
import ast
import calendar
import getpass
import logging
import os
import random
import sys
import time
from datetime import datetime
from urllib import unquote

import ee
import requests
import retrying
from bs4 import BeautifulSoup

# Importing the credentials provided as JSON but ignored in code commits
import credentials as cr

class DataManagement(object):

    """Algorithm for managing Water Productivity assets in GEE"""

    EE_CREDENTIALS = ee.ServiceAccountCredentials ( cr.EE_ACCOUNT , cr.EE_PRIVATE_KEY_FILE , cr.GOOGLE_SERVICE_ACCOUNT_SCOPES )
    ee.Initialize (EE_CREDENTIALS)

    #Initialization with email and password
    #ee.Initialize()

    # Define URLs
    __GOOGLE_ACCOUNT_URL = 'https://accounts.google.com'
    __AUTHENTICATION_URL = 'https://accounts.google.com/ServiceLoginAuth'
    __APPSPOT_URL = 'https://ee-api.appspot.com/assets/upload/geturl?'
    __REGION_L1 = ee.Geometry.Polygon([[[-30, -40], [65, -30], [65, 40], [-30, 40]]])
    __REGION = [[-25.0, -37.0], [60.0, -41.0], [58.0, 39.0], [-31.0, 38.0], [-25.0, -37.0]]

    # TODO: Is this the optimal file for calculating the scale?
    image_for_scale_calculation = ee.Image("projects/fao-wapor/L1_TFRAC/L1_TFRAC_1003")

    # Get scale (in meters) information from first image
    __SCALE_CALC = image_for_scale_calculation.projection().nominalScale ().getInfo ()
    dataManagement_logger = logging.getLogger ( "wpWin.DataManagement" )

    def __init__(self, usr, pwd):

        self.__username = usr
        self.__password = pwd

        self.asset_path = None
        #self.asset_path_root = 'projects/fao-wapor/'
        self.asset_path_root = 'users/fabiolananotizie/'

        self.dataManagement_logger.setLevel ( level=logging.DEBUG )
        formatter = logging.Formatter("%(levelname) -4s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s")

        fh = logging.FileHandler('log_files/wapor_gee_datamanagement.log')
        fh.setLevel(logging.ERROR)
        fh.setFormatter ( formatter )
        self.dataManagement_logger.addHandler(fh )

        ch = logging.StreamHandler ( sys.stdout )
        ch.setLevel ( logging.DEBUG )
        ch.setFormatter ( formatter )
        self.dataManagement_logger.addHandler ( ch )

        self.dataManagement_logger.debug('Scale used for Level 1 exports %f ' % DataManagement.__SCALE_CALC)

    @property
    def user_name(self):
        return self.__username

    @user_name.setter
    def user_name(self , name):

        self.__username = name

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, pwd):
        self.__password = pwd

    def create_google_session_oauth2(self):
        pass

    def create_google_session(self):

        '''
        Create a session using the credentials provided. The session is used throughout the code for a
        number of different purposes.

        :return: the active session for a specific user with specific credentials
        '''

        session = requests.session ()
        login_html = session.get ( DataManagement.__GOOGLE_ACCOUNT_URL )

        try:
            galx = session.cookies['GALX']
        except:
            self.dataManagement_logger.info( 'No cookie GALX' )

        soup_login = BeautifulSoup ( login_html.content , 'html.parser').find('form').find_all('input')
        payload = {}
        for u in soup_login:
            if u.has_attr('value'):
                payload[u['name']] = u['value']

        payload['Email'] = self.__username
        payload['Passwd'] = self.__password

        auto = login_html.headers.get('X-Auto-Login' )
        follow_up = unquote(unquote(auto) ).split ( 'continue=' )[-1]
        payload['continue'] = follow_up

        session.post(DataManagement.__AUTHENTICATION_URL, data=payload)
        return session

    def get_assets_info(self, id_name):

        """
        Method for getting information from existing GEE assets for assessing the need to delete
        existing data before uploading a new one.

        :type id_name: name of the asset in GEE (e.g. L1_AET)
        """
        asset_list = ['AET', 'NPP', 'AGBP', 'RET', 'PCP', 'LCC', 'TFRAC', 'PHE']
        asset_position = filter(lambda title: title in id_name, asset_list)[0]
        asset_position = id_name.index(asset_position) + len(asset_position)
        asset_name = id_name[0:asset_position]

        self.asset_path = self.asset_path_root + asset_name

        if ee.data.getInfo (self.asset_path):
            self.dataManagement_logger.debug('Collection %s Exists' % self.asset_path)
        else:
            ee.data.createAsset({'type': ee.data.ASSET_TYPE_IMAGE_COLL},  self.asset_path)
            self.dataManagement_logger.debug('Collection %s Created' % self.asset_path)

        ids_items_in_assets = ee.data.getList(params={'id':  self.asset_path})
        names_items_in_assets = [os.path.basename(asset['id']) for asset in ids_items_in_assets]

        return names_items_in_assets

    def get_upload_url(self, session):

        """
        Session is used for POSTing a new file to the right assets.Using a corporate account
        will likely change this part of the code

        :type session: Session generated using user and password
        """
        r = session.get ( DataManagement.__APPSPOT_URL )
        if r.text.startswith ( '\n<!DOCTYPE html>' ):
            self.dataManagement_logger.debug ( 'Incorrect credentials. Probably. If you are sure the credentials are OK, '
                                'refresh the authentication token. If it did not work report a problem. '
                                'They might have changed something in the Matrix.' )
            sys.exit ( 1 )
        elif r.text.startswith ( '<HTML>' ):
            self.dataManagement_logger.debug ( 'Redirecting to upload URL' )
            r = session.get ( DataManagement.__APPSPOT_URL )
            d = ast.literal_eval ( r.text )

        return d['url']

    def retry_if_ee_error(self, exception):
        """
        Retrying in case the first attempt do upload has failed.

        :rtype: object
        """
        return isinstance(exception, ee.EEException )

    def __delete_image(self, image):
        """
        This step will be performed if a previous version of the raster has already been uploaded

        :type image: neame of the image to be deleted
        """
        items_in_destination = ee.data.getList({'id': self.asset_path})
        for item in items_in_destination:
            asset_to_delete = item['id']
            if image in asset_to_delete:
                self.dataManagement_logger.warning('Found %s ' % image)
                ee.data.deleteAsset ( image )

    def __upload_image(self, file_path, session, upload_url, image_name, properties, nodata):
        '''
        Method for uploading the raster file into the appropriate asset

        :param file_path: where the file is stored on disk
        :param session: session created using the credentials
        :param upload_url: url used for uploading the raster
        :param image_name: nome of the resulting file
        :param properties: metadata of the file calculated automatically from file name
        :param nodata: no data either -9999 or 255 depending on the dataset
        :return: Nothing
        '''
        with open(file_path, 'rb') as f:
            files = {'file': f}
            resp = session.post(upload_url, files=files )
            gsid = resp.json()[0]
            asset_data = {"id": image_name ,
                          "tilesets": [
                              {"sources": [
                                  {"primaryPath": gsid ,
                                   "additionalPaths": []}
                              ]}
                          ] ,
                          "bands": [] ,
                          "properties": properties,
                          "missingData": {"value": nodata}
                          }
            try:
                task_id = ee.data.newTaskId ( 1 )[0]
                _ = ee.data.startIngestion ( task_id , asset_data )
            except IOError as ioe:
                self.dataManagement_logger.error(ioe)

    @retrying.retry ( retry_on_exception=retry_if_ee_error , wait_exponential_multiplier=1000 ,
                      wait_exponential_max=4000 , stop_max_attempt_number=3)
    def data_management(self, active_session, upload_url, images_already_in_asset, file_to_be_loaded,
                        name_image_to_load_in_asset, metadata, no_data):
        '''

        :param active_session: session created using the credentials
        :param upload_url: url used for uploading the raster
        :param images_already_in_asset: if the file is already stored in the GEE asset
        :param file_to_be_loaded: file name of the file to be loaded
        :param name_image_to_load_in_asset: name once the file is stored in asset
        :param metadata: metadata of the file calculated automatically from file name
        :param no_data: no data either -9999 or 255 depending on the dataset
        :return: Nothing
        '''

        final_image_name_in_asset = self.asset_path + '/%s' % name_image_to_load_in_asset

        already_uploaded = False
        if name_image_to_load_in_asset in images_already_in_asset:
            self.dataManagement_logger.error("%s already in collection" % name_image_to_load_in_asset)
            already_uploaded = True

        if os.path.exists(file_to_be_loaded) and not already_uploaded:
            self.__upload_image(file_to_be_loaded, active_session, upload_url, final_image_name_in_asset, metadata, no_data)
        else:
            self.dataManagement_logger.debug('%s already uploaded in GEE - Deleting old file' % name_image_to_load_in_asset)
            self.__delete_image(final_image_name_in_asset)
            self.__upload_image(file_to_be_loaded, active_session, upload_url, final_image_name_in_asset, metadata, no_data)

    @staticmethod
    def get_days_in_dekad(year, dekad):
        '''
        Getting the number of days contained in a certain dekad

        :param year: year contained in the file name
        :param dekad: number of the dekad 1 to 36
        :return: number of days
        '''
        dekads_eleven = [3, 8, 15, 20, 24, 30, 36]
        if dekad == 6:
            days = calendar.monthrange ( year , 2 )[1] - 20
        elif dekad in dekads_eleven:
            days = 11
        else:
            days = 10
        return days

    @staticmethod
    def get_month_of_dekad(dekad):
        '''
        the month in which the dekad is contained

        :param dekad:
        :return: month as integer
        '''

        if dekad <= 3:
            month_dekad = 1
        elif dekad > 3 and dekad <= 6:
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

    @staticmethod
    def get_starting_day_of_dekad(year , month , dekad):
        '''
        The starting date of a dekad

        :param year:
        :param month:
        :param dekad:
        :return: Date as string
        '''
        first_dekads = range ( 1 , 37 , 3 )
        second_dekads = range ( 2 , 37 , 3 )
        thirds_dekads = range ( 3 , 37 , 3 )

        month_year = (str(month), str(year))
        final_date = None
        if dekad in first_dekads:
            final_date = ('01' ,) + month_year + ('12:00:00' ,)
        elif dekad in second_dekads:
            final_date = ('11' ,) + month_year + ('12:00:00' ,)
        elif dekad in thirds_dekads:
            final_date = ('21' ,) + month_year + ('12:00:00' ,)

        starting_day = '-'.join ( final_date )

        return starting_day

    @staticmethod
    def no_data_assessment(data_component_code):
        '''
        The data to be nullified during the upload to GEE

        :param data_component_code:
        :return: either -9999 or 255
        '''
        if 'AET' in data_component_code:
            no_data = 255
        elif 'NPP' in data_component_code:
            no_data = -9999
        elif 'AGBP' in data_component_code:
            no_data = -9999
        elif 'RET' in data_component_code:
            no_data = 255
        elif 'PCP' in data_component_code:
            no_data = -9999
        elif 'LCC' in data_component_code:
            no_data = 255
        elif 'TFRAC' in data_component_code:
            no_data = 255
        elif 'PHE' in data_component_code:
            no_data = 255
        else:
            no_data = None

        return no_data

    @staticmethod
    def metadata_generator(gee_file, level, seasonal=False, area='AfNE'):
        '''
        Generated the metadata for a file using the file name

        :param gee_file: file to be uploaded to GEE
        :param level: level 1,2 or 3
        :param seasonal: in case the filename contains a season
        :param area: all of Africa or just east and west africa
        :return: dictionary containing the metadata
        '''
        initial_date = datetime.strptime('01-01-1970', "%d-%m-%Y" )

        id_no = os.path.basename(gee_file).split(".")[0]
        #level = int(re.split('(\d.*)', gee_file.split("_")[0])[1])

        if seasonal:
            seasons_list = ['s1', 's2']
            season_position = map(lambda season: id_no.find(season), seasons_list)[0]
            date_part = id_no.split('_')[-2]
            season_numeric = int(id_no[season_position+1:season_position+2])
            season_time = id_no.split('_')[-1]
            year = int('20' + date_part[0:2])
            area_part = id_no.split('_')[1]
            start_date = datetime.strptime("01-01-%s 12:00:00" % str(year), "%d-%m-%Y %H:%M:%S")
            unix_time_manual = int( (start_date - initial_date).total_seconds()) * 1000
            metadata = {'id_no': id_no , 'season': season_numeric, "area": area_part,
                        'system:time_start': unix_time_manual,
                        'year': year, "season_time": season_time, 'level': level}
        else:
            date_part = id_no.split('_')[-1]
            if len(date_part) == 4:
                year = int('20' + date_part[0:2] )
                dekad = int( date_part[2:4] )
                number_of_days = DataManagement.get_days_in_dekad ( year , dekad )
                month = DataManagement.get_month_of_dekad ( dekad )
                start_date = datetime.strptime(DataManagement.get_starting_day_of_dekad ( year , month, dekad),
                                                 "%d-%m-%Y-%H:%M:%S" )
                unix_time_manual = (start_date - initial_date).total_seconds() * 1000
                metadata = {'id_no': id_no, 'system:time_start': unix_time_manual,
                            'days_in_dk': number_of_days, 'level': level, 'area': area}
            elif len(date_part) == 6:
                year = int('20' + id_no.split('_')[2][0:2])
                month = int(id_no.split('_')[2][2:4])
                day = int(id_no.split('_')[2][4:6])
                s = str(day) + "-" + str(month) + "-" + str(year) + "-" + '12:00:00'
                start_date = datetime.strptime(s, "%d-%m-%Y-%H:%M:%S")
                unix_time_manual = (start_date - initial_date).total_seconds () * 1000
                metadata = {'id_no': id_no, 'system:time_start': unix_time_manual , 'level': level , 'area': area}

        return metadata

    @classmethod
    def image_export(cls, exp_type, aggregated_layer, data_name, date_start, date_end):

        '''
        Export of the calculated Water Productivity to Google Drive or GEE Asset

        :param exp_type:
        :param aggregated_layer:
        :param data_name:
        :param date_start:
        :param date_end:
        :return:
        '''

        root = '_'.join (
            [data_name.replace ( "_" , "" ) , date_start.replace ( "-" , "" ) , date_end.replace ( "-" , "" )] )

        if exp_type == 'drive':

            #Reducing the size of the file
            WPbm_1000 = aggregated_layer.multiply ( 1000 )
            WPbm_1000_int_32 = WPbm_1000.toInt32 ()

            #Unmasking for Geoserver
            WPbm_unmask = WPbm_1000_int_32.unmask ( -9999 )

            task = ee.batch.Export.image.toDrive (
                        image=WPbm_unmask ,
                        description=root ,
                        crs="EPSG:4326" ,
                        scale= cls.__SCALE_CALC ,
                        region=cls.__REGION,
                        folder= root ,
                        maxPixels= 9000000000
            )
            task.start ()
            while task.status ()['state'] == 'RUNNING':
                #Perhaps task.cancel() at some point.
                time.sleep ( 1 )
            cls.dataManagement_logger.debug('Download running....')

        elif exp_type == 'asset':

            ee.batch.Export.image.toAsset (
                            image=aggregated_layer ,
                            description=root ,
                            assetId='users/fabiolananotizie/' + root,
                            crs="EPSG:4326" ,
                            scale=cls.__SCALE_CALC ,
                            region=cls.__REGION,
                            maxPixels= 9000000000
                ).start ()
        else:
            print("Nothing yet")
            pass

    @staticmethod
    def get_tasks_list():
        '''
        Check if any file is being uploaded

        :return: list of tasks
        '''
        return ee.data.getTaskList()

    @staticmethod
    def get_tasks():
        '''
        Check if any file is being uploaded

        :return: list of tasks
        '''
        return ee.batch.Task.list ()

    @staticmethod
    def running_tasks(tasks):
        '''
        List of tasks running at the moment of the request

        :param tasks:
        :return: tasks active and what is being uploaded
        '''

        tasks_running = [task for task in tasks if task.config['state'] in (u'RUNNING' , u'UNSUBMITTED' , u'READY')]

        name_asset_in_upload = []
        if len ( tasks_running ) > 0:
            for running in tasks_running:
                status = running.status ()
                name_asset_in_upload.append(status['description'])

        return tasks_running, name_asset_in_upload

    def completed_cancelled_tasks(self, tasks):
        '''
        Tasks completed or cancelled by the user not used but useful

        :param tasks:
        :return:
        '''
        tasks_completed_cancelled = [task for task in tasks if task.config['state'] in (u'COMPLETED',u'CANCELLED')]
        return tasks_completed_cancelled

    def failed_tasks(self,tasks):
        '''
        Tasks failed not used but useful

        :param tasks:
        :return:
        '''
        tasks_failed = [task for task in tasks if task.config['state'] in (u'FAILED')]

        for failed in tasks_failed:
            task_id = failed.status()['id']
            image_id = failed.status ()['description']
            error_message = failed.status()['error_message']
            print( 'Ingestion of image %s with id %s has failed with message %s' % (image_id ,task_id , error_message ))

    def cancel_task(self,task):
        '''
        This method can stop any active uploading not used but useful

        :param task:
        :return:
        '''

        random_time = random.random ()
        time.sleep ( 0.5 + random_time * 0.5 )
        if task.config['state'] in (u'RUNNING' , u'UNSUBMITTED' , u'READY'):
            print 'cancelling %s' % task
            task.cancel ()

def main(argv):

    if len(argv) != 1:
        print('\nSend a filename or directory\n')
        sys.exit('Usage: wpDataManagement.py <directory_name>')

    username_gee = raw_input('Please Enter a Valid GEE User Name (without @gmail.com): ') + "@gmail.com"
    password_gee = getpass.getpass('Please Enter a Valid GEE Password: ')

    data_management_session = DataManagement(username_gee, password_gee)
    active_session = data_management_session.create_google_session()
    upload_url = data_management_session.get_upload_url(active_session)

    path_or_file = argv[0]

    list_of_files_in_process = []
    if os.path.isfile(path_or_file):
        list_of_files_in_process.append(path_or_file)
    elif os.path.isdir(path_or_file):
        for (dirpath , dirnames , filenames) in os.walk(path_or_file):
            list_of_files_in_process.extend(dirpath + "/" + filenames)
            break

    metadata_list = []
    for active_file in list_of_files_in_process:
        no_data = DataManagement.no_data_assessment(active_file)
        if "L1" in active_file:
            metadata = DataManagement.metadata_generator(active_file, 1)
            metadata_list.append(metadata)
        elif "L2" in active_file:
            if "s1" in active_file or "s2" in active_file:
                metadata = DataManagement.metadata_generator(active_file, 2, seasonal=True)
                metadata_list.append(metadata)
            else:
                metadata = DataManagement.metadata_generator(active_file)
                metadata_list.append(metadata)
        name_image_in_asset = metadata['id_no']
        data_management_session.dataManagement_logger.info('Properties %s ' % str(metadata))
        data_management_session.dataManagement_logger.info('No data %d ' % no_data)
        list_of_images_already_in_asset = data_management_session.get_assets_info(name_image_in_asset)

        data_management_session.data_management(active_session,
                                                upload_url,
                                                list_of_images_already_in_asset,
                                                active_file,
                                                name_image_in_asset,
                                                metadata,
                                                no_data)

if __name__ == "__main__":
    main(sys.argv[1:])